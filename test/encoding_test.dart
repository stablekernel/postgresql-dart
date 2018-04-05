import 'dart:async';
import 'dart:convert';
import 'package:postgres/postgres.dart';
import 'package:postgres/src/binary_codec.dart';
import 'package:postgres/src/text_codec.dart';
import 'package:test/test.dart';
import 'package:postgres/src/types.dart';
import 'package:postgres/src/utf8_backed_string.dart';

PostgreSQLConnection conn;

void main() {
  group("Binary encoders", () {
    setUp(() async {
      conn = new PostgreSQLConnection("localhost", 5432, "dart_test", username: "dart", password: "dart");
      await conn.open();
    });

    tearDown(() async {
      await conn.close();
      conn = null;
    });

    test("bool", () async {
      await expectInverse(true, PostgreSQLDataType.boolean);
      await expectInverse(false, PostgreSQLDataType.boolean);
    });

    test("smallint", () async {
      await expectInverse(-1, PostgreSQLDataType.smallInteger);
      await expectInverse(0, PostgreSQLDataType.smallInteger);
      await expectInverse(1, PostgreSQLDataType.smallInteger);
    });

    test("integer", () async {
      await expectInverse(-1, PostgreSQLDataType.integer);
      await expectInverse(0, PostgreSQLDataType.integer);
      await expectInverse(1, PostgreSQLDataType.integer);
    });

    test("serial", () async {
      await expectInverse(0, PostgreSQLDataType.serial);
      await expectInverse(1, PostgreSQLDataType.serial);
    });

    test("bigint", () async {
      await expectInverse(-1, PostgreSQLDataType.bigInteger);
      await expectInverse(0, PostgreSQLDataType.bigInteger);
      await expectInverse(1, PostgreSQLDataType.bigInteger);
    });

    test("bigserial", () async {
      await expectInverse(0, PostgreSQLDataType.bigSerial);
      await expectInverse(1, PostgreSQLDataType.bigSerial);
    });

    test("text", () async {
      await expectInverse("", PostgreSQLDataType.text);
      await expectInverse("foo", PostgreSQLDataType.text);
      await expectInverse("foo\n", PostgreSQLDataType.text);
      await expectInverse("foo\nbar;s", PostgreSQLDataType.text);
    });

    test("real", () async {
      await expectInverse(-1.0, PostgreSQLDataType.real);
      await expectInverse(0.0, PostgreSQLDataType.real);
      await expectInverse(1.0, PostgreSQLDataType.real);
    });

    test("double", () async {
      await expectInverse(-1.0, PostgreSQLDataType.double);
      await expectInverse(0.0, PostgreSQLDataType.double);
      await expectInverse(1.0, PostgreSQLDataType.double);
    });

    test("data", () async {
      await expectInverse(new DateTime.utc(1920, 10, 1), PostgreSQLDataType.date);
      await expectInverse(new DateTime.utc(2120, 10, 5), PostgreSQLDataType.date);
      await expectInverse(new DateTime.utc(2016, 10, 1), PostgreSQLDataType.date);
    });

    test("timestamp", () async {
      await expectInverse(new DateTime.utc(1920, 10, 1), PostgreSQLDataType.timestampWithoutTimezone);
      await expectInverse(new DateTime.utc(2120, 10, 5), PostgreSQLDataType.timestampWithoutTimezone);
    });

    test("timestamptz", () async {
      await expectInverse(new DateTime.utc(1920, 10, 1), PostgreSQLDataType.timestampWithTimezone);
      await expectInverse(new DateTime.utc(2120, 10, 5), PostgreSQLDataType.timestampWithTimezone);
    });

    test("jsonb", () async {
      await expectInverse("string", PostgreSQLDataType.json);
      await expectInverse(2, PostgreSQLDataType.json);
      await expectInverse(["foo"], PostgreSQLDataType.json);
      await expectInverse({
        "key": "val",
        "key1": 1,
        "array": ["foo"]
      }, PostgreSQLDataType.json);
    });

    test("bytea", () async {
      await expectInverse([0], PostgreSQLDataType.byteArray);
      await expectInverse([1,2,3,4,5], PostgreSQLDataType.byteArray);
      await expectInverse([255, 254, 253], PostgreSQLDataType.byteArray);
    });
  });

  group("Text encoders", () {
    test("Escape strings", () {
      final encoder = new PostgresTextEncoder(true);
      //                                                       '   b   o    b   '
      expect(UTF8.encode(encoder.convert('bob')), equals([39, 98, 111, 98, 39]));

      //                                                         '   b   o   \n   b   '
      expect(UTF8.encode(encoder.convert('bo\nb')), equals([39, 98, 111, 10, 98, 39]));

      //                                                         '   b   o   \r   b   '
      expect(UTF8.encode(encoder.convert('bo\rb')), equals([39, 98, 111, 13, 98, 39]));

      //                                                         '   b   o  \b   b   '
      expect(UTF8.encode(encoder.convert('bo\bb')), equals([39, 98, 111, 8, 98, 39]));

      //                                                     '   '   '   '
      expect(UTF8.encode(encoder.convert("'")), equals([39, 39, 39, 39]));

      //                                                      '   '   '   '   '   '
      expect(UTF8.encode(encoder.convert("''")), equals([39, 39, 39, 39, 39, 39]));

      //                                                       '   '   '   '   '   '
      expect(UTF8.encode(encoder.convert("\''")), equals([39, 39, 39, 39, 39, 39]));

      //                                                       sp   E   '   \   \   '   '   '   '   '
      expect(UTF8.encode(encoder.convert("\\''")), equals([32, 69, 39, 92, 92, 39, 39, 39, 39, 39]));

      //                                                      sp   E   '   \   \   '   '   '
      expect(UTF8.encode(encoder.convert("\\'")), equals([32, 69, 39, 92, 92, 39, 39, 39]));
    });

    test("Encode DateTime", () {
      // Get users current timezone
      var tz = new DateTime(2001, 2, 3).timeZoneOffset;
      var tzOffsetDelimiter = "${tz.isNegative ? '-' : '+'}"
          "${tz
        .abs()
        .inHours
        .toString()
        .padLeft(2, '0')}"
          ":${(tz.inSeconds % 60).toString().padLeft(2, '0')}";

      var pairs = {
        "2001-02-03T00:00:00.000$tzOffsetDelimiter": new DateTime(2001, DateTime.FEBRUARY, 3),
        "2001-02-03T04:05:06.000$tzOffsetDelimiter": new DateTime(2001, DateTime.FEBRUARY, 3, 4, 5, 6, 0),
        "2001-02-03T04:05:06.999$tzOffsetDelimiter": new DateTime(2001, DateTime.FEBRUARY, 3, 4, 5, 6, 999),
        "0010-02-03T04:05:06.123$tzOffsetDelimiter BC": new DateTime(-10, DateTime.FEBRUARY, 3, 4, 5, 6, 123),
        "0010-02-03T04:05:06.000$tzOffsetDelimiter BC": new DateTime(-10, DateTime.FEBRUARY, 3, 4, 5, 6, 0),
        "012345-02-03T04:05:06.000$tzOffsetDelimiter BC": new DateTime(-12345, DateTime.FEBRUARY, 3, 4, 5, 6, 0),
        "012345-02-03T04:05:06.000$tzOffsetDelimiter": new DateTime(12345, DateTime.FEBRUARY, 3, 4, 5, 6, 0)
      };

      final encoder = new PostgresTextEncoder(false);
      pairs.forEach((k, v) {
        expect(encoder.convert(v), "'$k'");
      });
    });

    test("Encode Double", () {
      var pairs = {
        "'nan'": double.NAN,
        "'infinity'": double.INFINITY,
        "'-infinity'": double.NEGATIVE_INFINITY,
        "1.7976931348623157e+308": double.MAX_FINITE,
        "5e-324": double.MIN_POSITIVE,
        "-0.0": -0.0,
        "0.0": 0.0
      };

      final encoder = new PostgresTextEncoder(false);
      pairs.forEach((k, v) {
        expect(encoder.convert(v), "$k");
      });
    });

    test("Encode Int", () {
      final encoder = new PostgresTextEncoder(false);

      expect(encoder.convert(1), "1");
      expect(encoder.convert(1234324323), "1234324323");
      expect(encoder.convert(-1234324323), "-1234324323");
    });

    test("Encode Bool", () {
      final encoder = new PostgresTextEncoder(false);

      expect(encoder.convert(true), "TRUE");
      expect(encoder.convert(false), "FALSE");
    });

    test("Encode JSONB", () {
      final encoder = new PostgresTextEncoder(false);

      expect(encoder.convert({"a": "b"}), "{\"a\":\"b\"}");
      expect(encoder.convert({"a": true}), "{\"a\":true}");
      expect(encoder.convert({"b": false}), "{\"b\":false}");
    });
  });

  test("UTF8String caches string regardless of which method is called first", () {
    var u = new UTF8BackedString("abcd");
    var v = new UTF8BackedString("abcd");

    u.utf8Length;
    v.utf8Bytes;

    expect(u.hasCachedBytes, true);
    expect(v.hasCachedBytes, true);
  });
}

Future expectInverse(dynamic value, PostgreSQLDataType dataType) async {
  final type = PostgreSQLFormat.dataTypeStringForDataType(dataType);

  await conn.execute("CREATE TEMPORARY TABLE IF NOT EXISTS t (v $type)");
  final result = await conn.query("INSERT INTO t (v) VALUES (${PostgreSQLFormat.id("v", type: dataType)}) RETURNING v", substitutionValues: {
    "v": value
  });
  expect(result.first.first, equals(value));

  final encoder = new PostgresBinaryEncoder(dataType);
  final encodedValue = encoder.convert(value);

  if (dataType == PostgreSQLDataType.serial) {
    dataType = PostgreSQLDataType.integer;
  } else if (dataType == PostgreSQLDataType.bigSerial) {
    dataType = PostgreSQLDataType.bigInteger;
  }
  var code;
  PostgresBinaryDecoder.typeMap.forEach((key, type) {
    if (type == dataType) {
      code = key;
    }
  });

  final decoder = new PostgresBinaryDecoder(code);
  final decodedValue = decoder.convert(encodedValue);

  expect(decodedValue, value);
}
