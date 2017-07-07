import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

void main() {
  group("Successful notifications", () {
    int maxConnection = 5;
    PostgreSQLConnectionPool pool;

    setUp(() async {
      pool = new PostgreSQLConnectionPool(maxConnection, "localhost", 5432, "dart_test",
          username: "dart", password: "dart");
      await pool.open();
    });

    tearDown(() async {
      await pool.close();
    });

    test("Pool connection empty test", () async {
    });

    test("Pool connection one connection", () async {
      //expect(await pool.connection.execute("select 1"), equals(1));
    });

    test("Pool connection more max connection", () async {
//      List<Connection> connections = new List<Connection>();
//      for(int i = 0; i< maxConnection;i++) {
//        connections.add(await pool.getConnection());
//      }
//
//      bool isTimeout = false;
//      try {
//        await pool.getConnection(timeout: new Duration(seconds: 1));
//      }
//      catch (_) {
//        isTimeout = true;
//      }
//      expect(isTimeout, true);
//      for(var connection in connections)
//        connection.close();
    });
  });
}
