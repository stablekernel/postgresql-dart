library postgres.pool;

import 'dart:async';
import 'dart:collection';

import 'package:postgres/postgres.dart';
import 'package:postgres/src/server_messages.dart';


class _PooledConnection implements Connection {

  _PooledConnection._internal(this._pool, this._connection){
    _subscription = _connection.notifications.listen((onData){
      _notifications.add(onData);
    });
  }

  final ConnectionPool _pool;
  final PostgreSQLConnection _connection;
  StreamSubscription<Notification> _subscription;

  bool _isClosed = false;
  final StreamController<Notification> _notifications = new StreamController<Notification>.broadcast();

  _error(String fnName) => new Exception('$fnName() called on closed connection.');

  bool get isClosed => _isClosed || _connection.isClosed;

  Stream<Notification> get notifications => _isClosed ?
    _error("notification"):
    _notifications;

  int get processID => _isClosed ?
    _error("processID"):
    _connection.processID;

  Future<int> execute(String fmtString, {Map<String, dynamic> substitutionValues: null}) {
    if(_isClosed) {
      _error("execute");
    }

    return _connection.execute(fmtString, substitutionValues: substitutionValues);
  }

  Future<List<List>> query(String fmtString,
      {Map<String, dynamic> substitutionValues: null, bool allowReuse: true}) {
    if(_isClosed) {
      _error("query");
    }

    return _connection.query(fmtString,
        substitutionValues: substitutionValues, allowReuse: allowReuse);
  }

  Future transaction(Future queryBlock(PostgreSQLExecutionContext connection)) {
    if(_isClosed) {
      _error("transaction");
    }

    return _connection.transaction(queryBlock);
  }

  void cancelTransaction({String reason: null}) {
    if(_isClosed) {
      _error("cancelTransaction");
    }

    _connection.cancelTransaction(reason: reason);
  }

  Future close() async {
    if(!_isClosed) {
      await _subscription.cancel();
      _pool._releaseConnection(this);
      _isClosed = true;
    }
    return new Future.value(true);
  }
}

class ConnectionPool {
  /// Creates an instance of [ConnectionPool].
  ///
  /// [host] must be a hostname, e.g. "foobar.com" or IP address. Do not include scheme or port.
  /// [port] is the port to connect to the database on. It is typically 5432 for default PostgreSQL settings.
  /// [databaseName] is the name of the database to connect to.
  /// [username] and [password] are optional if the database requires user authentication.
  /// [timeoutInSeconds] refers to the amount of time [PostgreSQLConnection] will wait while establishing a connection before it gives up.
  /// [timeZone] is the timezone the connection is in. Defaults to 'UTC'.
  /// [useSSL] when true, uses a secure socket when connecting to a PostgreSQL database.
  /// [maxRetryDelay] Maximum wait time between connection attempts
  /// [maxCountConnectionInPool] maximum count available connection it PostgreSQL database.
  /// [useHeartbeat] when true, uses [Timer.periodic] for check available connections.
  ConnectionPool(this.host, this.port, this.databaseName,
      {this.username: null,
        this.password: null,
        this.timeoutInSeconds: 30,
        this.timeZone: "UTC",
        this.useSSL: false,
        this.maxRetryDelay,
        this.maxCountConnectionInPool: 10,
        bool useHeartbeat: true,
        Duration heartbeatInterval}) {
    _createConnections();
    if(useHeartbeat) {
      heartbeatInterval = heartbeatInterval ?? new Duration(seconds: 1);
      _heartbeatTimer = new Timer.periodic(
          heartbeatInterval, (_) async => await _heartbeat());
    }
    _resetRetryState();
  }

  /// Hostname of database this connection refers to.
  final String host;

  /// Port of database this connection refers to.
  final int port;

  /// Name of database this connection refers to.
  final String databaseName;

  /// Username for authenticating this connection.
  final String username;

  /// Password for authenticating this connection.
  final String password;

  /// The amount of time this connection will wait during connecting before giving up.
  final int timeoutInSeconds;

  /// The timezone of this connection for date operations that don't specify a timezone.
  final String timeZone;

  /// Whether or not this connection should connect securely.
  final bool useSSL;

  /// Maximum count available connection it PostgreSQL database.
  final int maxCountConnectionInPool;

  /// Maximum wait time between attempts to restore connections
  final Duration maxRetryDelay;

  final Queue<_PooledConnection> _availableConnections = new Queue<_PooledConnection>();
  final Set<_PooledConnection> _busyConnections = new Set<_PooledConnection>();
  final Queue<Completer<_PooledConnection>> _waitQueue = new Queue<Completer<_PooledConnection>>();
  Timer _heartbeatTimer;
  Duration _currentRetryDelay;
  bool _creatingConnections = false;

  /// Get connection from pool
  ///
  /// If this method does not complete before `timeout` has passed,
  /// will cause the returned future to complete with a [TimeoutException].
  Future<Connection> getConnection({Duration timeout}) async {
    Connection connection = _getConnection();
    if(connection != null) {
      return new Future.value(connection);
    }

    Completer<_PooledConnection> completer = new Completer<_PooledConnection>();
    _waitQueue.addLast(completer);
    try {
      Future<_PooledConnection> future = completer.future;
      if(timeout != null) {
        future = future.timeout(timeout);
      }
      return await future;
    }
    on TimeoutException catch (_) {
      _waitQueue.remove(completer);
      rethrow;
    }
  }

  /// Stop connection pool
  /// All busy connections must be closed
  void stop() {
    if(_busyConnections.isNotEmpty)
      throw new Exception("Not all connections have been released.");

    _heartbeatTimer?.cancel();

    while(_availableConnections.isNotEmpty) {
      _PooledConnection connection = _availableConnections.removeFirst();
      connection.close();
    }
  }

  Future _heartbeat() async {
    for (int i=0; i< _availableConnections.length; i++) {
      try {
        _PooledConnection connection = _availableConnections.removeFirst();
        await connection.execute("SELECT 1;");
        _availableConnections.addLast(connection);
      }
      catch (e) {}
    }
    if(_availableConnections.length + _busyConnections.length < maxCountConnectionInPool &&
      !_creatingConnections) {
      _createConnections();
    }
  }

  void _createConnections() {
    scheduleMicrotask(() async {
      try {
        while(_availableConnections.length + _busyConnections.length < maxCountConnectionInPool) {
            PostgreSQLConnection newConnection = new PostgreSQLConnection(
                host, port,
                databaseName, username: username,
                password: password,
                timeoutInSeconds: timeoutInSeconds,
                timeZone: timeZone,
                useSSL: useSSL);
            await newConnection.open();
            _addInAvailable(new _PooledConnection._internal(this, newConnection));
        }
        _resetRetryState();
        _creatingConnections = false;
      }
      catch (_){
        _creatingConnections = true;
        Duration retryDelay = _getRetryDelay();
        await new Future.delayed(retryDelay);
        _createConnections();
      }
    });
  }

  void _addInAvailable(_PooledConnection _connection) {
    _availableConnections.addLast(_connection);
    while(_availableConnections.isNotEmpty && _waitQueue.isNotEmpty) {
      _PooledConnection connection = _getConnection();
      if(connection == null) {
        break;
      }

      Completer<_PooledConnection> completer = _waitQueue.removeFirst();
      completer.complete(connection);
    }
  }

  _PooledConnection _getConnection() {
    if(_availableConnections.isEmpty)
      return null;

    var connection = _availableConnections.removeFirst();
    _busyConnections.add(connection);
    return connection;
  }

  void _releaseConnection(_PooledConnection connection) {
    _busyConnections.remove(connection);

    if(connection.isClosed) {
      if(!_creatingConnections)
        _createConnections();
    }
    else {
      _addInAvailable(connection);
    }
  }

  Duration _getRetryDelay() {
    _currentRetryDelay = new Duration(milliseconds: _currentRetryDelay.inMilliseconds * 2);
    if(maxRetryDelay != null && _currentRetryDelay > maxRetryDelay) {
      _currentRetryDelay = maxRetryDelay;
    }

    return _currentRetryDelay;
  }

  void _resetRetryState() {
    _currentRetryDelay = new Duration(milliseconds: 1);
  }
}
