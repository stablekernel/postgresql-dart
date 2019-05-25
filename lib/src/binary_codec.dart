import 'dart:convert';
import 'dart:typed_data';

import 'package:buffer/buffer.dart';

import '../postgres.dart';
import 'types.dart';

class PostgresBinaryEncoder extends Converter<dynamic, Uint8List> {
  const PostgresBinaryEncoder(this.dataType);

  final PostgreSQLDataType dataType;

  @override
  Uint8List convert(dynamic value) {
    if (value == null) {
      return null;
    }

    switch (dataType) {
      case PostgreSQLDataType.boolean:
        {
          if (value is bool) {
            final bd = ByteData(1);
            bd.setUint8(0, value ? 1 : 0);
            return bd.buffer.asUint8List();
          }
          throw FormatException(
              'Invalid type for parameter value. Expected: bool Got: ${value.runtimeType}');
        }
      case PostgreSQLDataType.bigSerial:
      case PostgreSQLDataType.bigInteger:
        {
          if (value is int) {
            final bd = ByteData(8);
            bd.setInt64(0, value);
            return bd.buffer.asUint8List();
          }
          throw FormatException(
              'Invalid type for parameter value. Expected: int Got: ${value.runtimeType}');
        }
      case PostgreSQLDataType.serial:
      case PostgreSQLDataType.integer:
        {
          if (value is int) {
            final bd = ByteData(4);
            bd.setInt32(0, value);
            return bd.buffer.asUint8List();
          }
          throw FormatException(
              'Invalid type for parameter value. Expected: int Got: ${value.runtimeType}');
        }
      case PostgreSQLDataType.smallInteger:
        {
          if (value is int) {
            final bd = ByteData(2);
            bd.setInt16(0, value);
            return bd.buffer.asUint8List();
          }
          throw FormatException(
              'Invalid type for parameter value. Expected: int Got: ${value.runtimeType}');
        }
      case PostgreSQLDataType.name:
      case PostgreSQLDataType.text:
        {
          if (value is String) {
            return castBytes(utf8.encode(value));
          }
          throw FormatException(
              'Invalid type for parameter value. Expected: String Got: ${value.runtimeType}');
        }
      case PostgreSQLDataType.real:
        {
          if (value is double) {
            final bd = ByteData(4);
            bd.setFloat32(0, value);
            return bd.buffer.asUint8List();
          }
          throw FormatException(
              'Invalid type for parameter value. Expected: double Got: ${value.runtimeType}');
        }
      case PostgreSQLDataType.double:
        {
          if (value is double) {
            final bd = ByteData(8);
            bd.setFloat64(0, value);
            return bd.buffer.asUint8List();
          }
          throw FormatException(
              'Invalid type for parameter value. Expected: double Got: ${value.runtimeType}');
        }
      case PostgreSQLDataType.date:
        {
          if (value is DateTime) {
            final bd = ByteData(4);
            bd.setInt32(0, value.toUtc().difference(DateTime.utc(2000)).inDays);
            return bd.buffer.asUint8List();
          }
          throw FormatException(
              'Invalid type for parameter value. Expected: DateTime Got: ${value.runtimeType}');
        }

      case PostgreSQLDataType.timestampWithoutTimezone:
        {
          if (value is DateTime) {
            final bd = ByteData(8);
            final diff = value.toUtc().difference(DateTime.utc(2000));
            bd.setInt64(0, diff.inMicroseconds);
            return bd.buffer.asUint8List();
          }
          throw FormatException(
              'Invalid type for parameter value. Expected: DateTime Got: ${value.runtimeType}');
        }

      case PostgreSQLDataType.timestampWithTimezone:
        {
          if (value is DateTime) {
            final bd = ByteData(8);
            bd.setInt64(
                0, value.toUtc().difference(DateTime.utc(2000)).inMicroseconds);
            return bd.buffer.asUint8List();
          }
          throw FormatException(
              'Invalid type for parameter value. Expected: DateTime Got: ${value.runtimeType}');
        }

      case PostgreSQLDataType.json:
        {
          final jsonBytes = utf8.encode(json.encode(value));
          final outBuffer = Uint8List(jsonBytes.length + 1);
          outBuffer[0] = 1;
          for (var i = 0; i < jsonBytes.length; i++) {
            outBuffer[i + 1] = jsonBytes[i];
          }

          return outBuffer;
        }

      case PostgreSQLDataType.byteArray:
        {
          if (value is List<int>) {
            return castBytes(value);
          }
          throw FormatException(
              'Invalid type for parameter value. Expected: List<int> Got: ${value.runtimeType}');
        }

      case PostgreSQLDataType.uuid:
        {
          if (value is! String) {
            throw FormatException(
                'Invalid type for parameter value. Expected: String Got: ${value.runtimeType}');
          }

          final dashUnit = '-'.codeUnits.first;
          final hexBytes = (value as String)
              .toLowerCase()
              .codeUnits
              .where((c) => c != dashUnit)
              .toList();
          if (hexBytes.length != 32) {
            throw FormatException(
                "Invalid UUID string. There must be exactly 32 hexadecimal (0-9 and a-f) characters and any number of '-' characters.");
          }

          final byteConvert = (int charCode) {
            if (charCode >= 48 && charCode <= 57) {
              return charCode - 48;
            } else if (charCode >= 97 && charCode <= 102) {
              return charCode - 87;
            }

            throw FormatException(
                'Invalid UUID string. Contains non-hexadecimal character (0-9 and a-f).');
          };

          final outBuffer = Uint8List(16);
          for (var i = 0; i < hexBytes.length; i += 2) {
            final upperByte = byteConvert(hexBytes[i]);
            final lowerByte = byteConvert(hexBytes[i + 1]);

            outBuffer[i ~/ 2] = upperByte * 16 + lowerByte;
          }
          return outBuffer;
        }
    }

    throw PostgreSQLException('Unsupported datatype');
  }
}

class PostgresBinaryDecoder extends Converter<Uint8List, dynamic> {
  const PostgresBinaryDecoder(this.typeCode);

  final int typeCode;

  @override
  dynamic convert(Uint8List value) {
    final dataType = typeMap[typeCode];

    if (value == null) {
      return null;
    }

    final buffer =
        ByteData.view(value.buffer, value.offsetInBytes, value.lengthInBytes);

    switch (dataType) {
      case PostgreSQLDataType.name:
      case PostgreSQLDataType.text:
        return utf8.decode(
            value.buffer.asUint8List(value.offsetInBytes, value.lengthInBytes));
      case PostgreSQLDataType.boolean:
        return buffer.getInt8(0) != 0;
      case PostgreSQLDataType.smallInteger:
        return buffer.getInt16(0);
      case PostgreSQLDataType.serial:
      case PostgreSQLDataType.integer:
        return buffer.getInt32(0);
      case PostgreSQLDataType.bigSerial:
      case PostgreSQLDataType.bigInteger:
        return buffer.getInt64(0);
      case PostgreSQLDataType.real:
        return buffer.getFloat32(0);
      case PostgreSQLDataType.double:
        return buffer.getFloat64(0);
      case PostgreSQLDataType.timestampWithoutTimezone:
      case PostgreSQLDataType.timestampWithTimezone:
        return DateTime.utc(2000)
            .add(Duration(microseconds: buffer.getInt64(0)));

      case PostgreSQLDataType.date:
        return DateTime.utc(2000).add(Duration(days: buffer.getInt32(0)));

      case PostgreSQLDataType.json:
        {
          // Removes version which is first character and currently always '1'
          final bytes = value.buffer
              .asUint8List(value.offsetInBytes + 1, value.lengthInBytes - 1);
          return json.decode(utf8.decode(bytes));
        }

      case PostgreSQLDataType.byteArray:
        return value.buffer
            .asUint8List(value.offsetInBytes, value.lengthInBytes);

      case PostgreSQLDataType.uuid:
        {
          final codeDash = '-'.codeUnitAt(0);

          final cipher = [
            '0',
            '1',
            '2',
            '3',
            '4',
            '5',
            '6',
            '7',
            '8',
            '9',
            'a',
            'b',
            'c',
            'd',
            'e',
            'f'
          ];
          final byteConvert = (int value) {
            return cipher[value];
          };

          final buf = StringBuffer();
          for (var i = 0; i < buffer.lengthInBytes; i++) {
            final byteValue = buffer.getUint8(i);
            final upperByteValue = byteValue ~/ 16;

            final upperByteHex = byteConvert(upperByteValue);
            final lowerByteHex = byteConvert(byteValue - (upperByteValue * 16));
            buf.write(upperByteHex);
            buf.write(lowerByteHex);
            if (i == 3 || i == 5 || i == 7 || i == 9) {
              buf.writeCharCode(codeDash);
            }
          }

          return buf.toString();
        }
    }

    // We'll try and decode this as a utf8 string and return that
    // for many internal types, this is valid. If it fails,
    // we just return the bytes and let the caller figure out what to
    // do with it.
    try {
      return utf8.decode(value);
    } catch (_) {
      return value;
    }
  }

  static final Map<int, PostgreSQLDataType> typeMap = {
    16: PostgreSQLDataType.boolean,
    17: PostgreSQLDataType.byteArray,
    19: PostgreSQLDataType.name,
    20: PostgreSQLDataType.bigInteger,
    21: PostgreSQLDataType.smallInteger,
    23: PostgreSQLDataType.integer,
    25: PostgreSQLDataType.text,
    700: PostgreSQLDataType.real,
    701: PostgreSQLDataType.double,
    1082: PostgreSQLDataType.date,
    1114: PostgreSQLDataType.timestampWithoutTimezone,
    1184: PostgreSQLDataType.timestampWithTimezone,
    2950: PostgreSQLDataType.uuid,
    3802: PostgreSQLDataType.json,
  };
}
