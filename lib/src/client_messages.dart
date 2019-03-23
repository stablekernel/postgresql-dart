import 'dart:typed_data';

import 'package:buffer/buffer.dart';
import 'package:crypto/crypto.dart';

import 'constants.dart';
import 'query.dart';
import 'utf8_backed_string.dart';

abstract class ClientMessage {
  static const int FormatText = 0;
  static const int FormatBinary = 1;

  static const int ProtocolVersion = 196608;

  static const int BindIdentifier = 66;
  static const int DescribeIdentifier = 68;
  static const int ExecuteIdentifier = 69;
  static const int ParseIdentifier = 80;
  static const int QueryIdentifier = 81;
  static const int SyncIdentifier = 83;
  static const int PasswordIdentifier = 112;

  int get length;

  void applyStringToBuffer(UTF8BackedString string, ByteDataWriter buffer) {
    buffer.write(string.utf8Bytes);
    buffer.writeInt8(0);
  }

  void applyBytesToBuffer(List<int> bytes, ByteDataWriter buffer) {
    buffer.write(bytes);
  }

  void applyToBuffer(ByteDataWriter buffer);

  Uint8List asBytes() {
    final buffer = ByteDataWriter();
    applyToBuffer(buffer);
    return buffer.toBytes();
  }

  static Uint8List aggregateBytes(List<ClientMessage> messages) {
    final buffer = ByteDataWriter();
    messages.forEach((cm) => cm.applyToBuffer(buffer));
    return buffer.toBytes();
  }
}

class StartupMessage extends ClientMessage {
  final UTF8BackedString _username;
  final UTF8BackedString _databaseName;
  final UTF8BackedString _timeZone;

  StartupMessage(String databaseName, String timeZone, {String username})
      : _databaseName = UTF8BackedString(databaseName),
        _timeZone = UTF8BackedString(timeZone),
        _username = username == null ? null : UTF8BackedString(username);

  @override
  int get length {
    final fixedLength = 53;
    final variableLength = (_username?.utf8Length ?? 0) +
        _databaseName.utf8Length +
        _timeZone.utf8Length +
        3;

    return fixedLength + variableLength;
  }

  @override
  void applyToBuffer(ByteDataWriter buffer) {
    buffer.writeInt32(length);
    buffer.writeInt32(ClientMessage.ProtocolVersion);

    if (_username != null) {
      applyBytesToBuffer((UTF8ByteConstants.user), buffer);
      applyStringToBuffer(_username, buffer);
    }

    applyBytesToBuffer(UTF8ByteConstants.database, buffer);
    applyStringToBuffer(_databaseName, buffer);

    applyBytesToBuffer(UTF8ByteConstants.clientEncoding, buffer);
    applyBytesToBuffer(UTF8ByteConstants.utf8, buffer);

    applyBytesToBuffer(UTF8ByteConstants.timeZone, buffer);
    applyStringToBuffer(_timeZone, buffer);

    buffer.writeInt8(0);
  }
}

class AuthMD5Message extends ClientMessage {
  UTF8BackedString _hashedAuthString;

  AuthMD5Message(String username, String password, List<int> saltBytes) {
    final passwordHash = md5.convert('$password$username'.codeUnits).toString();
    final saltString = String.fromCharCodes(saltBytes);
    final md5Hash =
        md5.convert('$passwordHash$saltString'.codeUnits).toString();
    _hashedAuthString = UTF8BackedString('md5$md5Hash');
  }

  @override
  int get length {
    return 6 + _hashedAuthString.utf8Length;
  }

  @override
  void applyToBuffer(ByteDataWriter buffer) {
    buffer.writeUint8(ClientMessage.PasswordIdentifier);
    buffer.writeUint32(length - 1);
    applyStringToBuffer(_hashedAuthString, buffer);
  }
}

class QueryMessage extends ClientMessage {
  final UTF8BackedString _queryString;

  QueryMessage(String queryString)
      : _queryString = UTF8BackedString(queryString);

  @override
  int get length {
    return 6 + _queryString.utf8Length;
  }

  @override
  void applyToBuffer(ByteDataWriter buffer) {
    buffer.writeUint8(ClientMessage.QueryIdentifier);
    buffer.writeUint32(length - 1);
    applyStringToBuffer(_queryString, buffer);
  }
}

class ParseMessage extends ClientMessage {
  final UTF8BackedString _statementName;
  final UTF8BackedString _statement;

  ParseMessage(String statement, {String statementName = ''})
      : _statement = UTF8BackedString(statement),
        _statementName = UTF8BackedString(statementName);

  @override
  int get length {
    return 9 + _statement.utf8Length + _statementName.utf8Length;
  }

  @override
  void applyToBuffer(ByteDataWriter buffer) {
    buffer.writeUint8(ClientMessage.ParseIdentifier);
    buffer.writeUint32(length - 1);
    // Name of prepared statement
    applyStringToBuffer(_statementName, buffer);
    applyStringToBuffer(_statement, buffer); // Query string
    buffer.writeUint16(0);
  }
}

class DescribeMessage extends ClientMessage {
  final UTF8BackedString _statementName;

  DescribeMessage({String statementName = ''})
      : _statementName = UTF8BackedString(statementName);

  @override
  int get length {
    return 7 + _statementName.utf8Length;
  }

  @override
  void applyToBuffer(ByteDataWriter buffer) {
    buffer.writeUint8(ClientMessage.DescribeIdentifier);
    buffer.writeUint32(length - 1);
    buffer.writeUint8(83);
    applyStringToBuffer(_statementName, buffer); // Name of prepared statement
  }
}

class BindMessage extends ClientMessage {
  final List<ParameterValue> _parameters;
  final UTF8BackedString _statementName;
  final int _typeSpecCount;
  int _cachedLength;

  BindMessage(this._parameters, {String statementName = ''})
      : _typeSpecCount = _parameters.where((p) => p.isBinary).length,
        _statementName = UTF8BackedString(statementName);

  @override
  int get length {
    if (_cachedLength == null) {
      var inputParameterElementCount = _parameters.length;
      if (_typeSpecCount == _parameters.length || _typeSpecCount == 0) {
        inputParameterElementCount = 1;
      }

      _cachedLength = 15;
      _cachedLength += _statementName.utf8Length;
      _cachedLength += inputParameterElementCount * 2;
      _cachedLength +=
          _parameters.fold<int>(0, (len, ParameterValue paramValue) {
        if (paramValue.bytes == null) {
          return len + 4;
        } else {
          return len + 4 + paramValue.length;
        }
      });
    }
    return _cachedLength;
  }

  @override
  void applyToBuffer(ByteDataWriter buffer) {
    buffer.writeUint8(ClientMessage.BindIdentifier);
    buffer.writeUint32(length - 1);

    // Name of portal - currently unnamed portal.
    applyBytesToBuffer([0], buffer);
    // Name of prepared statement.
    applyStringToBuffer(_statementName, buffer);

    // OK, if we have no specified types at all, we can use 0. If we have all specified types, we can use 1. If we have a mix, we have to individually
    // call out each type.
    if (_typeSpecCount == _parameters.length) {
      buffer.writeUint16(1);
      // Apply following format code for all parameters by indicating 1
      buffer.writeUint16(ClientMessage.FormatBinary);
    } else if (_typeSpecCount == 0) {
      buffer.writeUint16(1);
      // Apply following format code for all parameters by indicating 1
      buffer.writeUint16(ClientMessage.FormatText);
    } else {
      // Well, we have some text and some binary, so we have to be explicit about each one
      buffer.writeUint16(_parameters.length);
      _parameters.forEach((p) {
        buffer.writeUint16(
            p.isBinary ? ClientMessage.FormatBinary : ClientMessage.FormatText);
      });
    }

    // This must be the number of $n's in the query.
    buffer.writeUint16(_parameters.length);
    _parameters.forEach((p) {
      if (p.bytes == null) {
        buffer.writeInt32(-1);
      } else {
        buffer.writeInt32(p.length);
        buffer.write(p.bytes);
      }
    });

    // Result columns - we always want binary for all of them, so specify 1:1.
    buffer.writeUint16(1);
    buffer.writeUint16(1);
  }
}

class ExecuteMessage extends ClientMessage {
  @override
  int get length {
    return 10;
  }

  @override
  void applyToBuffer(ByteDataWriter buffer) {
    buffer.writeUint8(ClientMessage.ExecuteIdentifier);
    buffer.writeUint32(length - 1);
    applyBytesToBuffer([0], buffer); // Portal name
    buffer.writeUint32(0);
  }
}

class SyncMessage extends ClientMessage {
  @override
  int get length {
    return 5;
  }

  @override
  void applyToBuffer(ByteDataWriter buffer) {
    buffer.writeUint8(ClientMessage.SyncIdentifier);
    buffer.writeUint32(4);
  }
}
