import 'dart:convert';
import 'package:test/test.dart';
import 'dart:typed_data';
import 'package:postgres/src/postgresql_codec.dart';
import 'package:postgres/src/utf8_backed_string.dart';

void main() {
  test("Binary encode/decode inverse", () {
    expectInverse(true, PostgreSQLCodec.TypeBool);
    expectInverse(false, PostgreSQLCodec.TypeBool);

    expectInverse(-1, PostgreSQLCodec.TypeInt2);
    expectInverse(0, PostgreSQLCodec.TypeInt2);
    expectInverse(1, PostgreSQLCodec.TypeInt2);

    expectInverse(-1, PostgreSQLCodec.TypeInt4);
    expectInverse(0, PostgreSQLCodec.TypeInt4);
    expectInverse(1, PostgreSQLCodec.TypeInt4);

    expectInverse(-1, PostgreSQLCodec.TypeInt8);
    expectInverse(0, PostgreSQLCodec.TypeInt8);
    expectInverse(1, PostgreSQLCodec.TypeInt8);

    expectInverse("", PostgreSQLCodec.TypeText);
    expectInverse("foo", PostgreSQLCodec.TypeText);
    expectInverse("foo\n", PostgreSQLCodec.TypeText);
    expectInverse("foo\nbar;s", PostgreSQLCodec.TypeText);

    expectInverse(-1.0, PostgreSQLCodec.TypeFloat4);
    expectInverse(0.0, PostgreSQLCodec.TypeFloat4);
    expectInverse(1.0, PostgreSQLCodec.TypeFloat4);

    expectInverse(-1.0, PostgreSQLCodec.TypeFloat8);
    expectInverse(0.0, PostgreSQLCodec.TypeFloat8);
    expectInverse(1.0, PostgreSQLCodec.TypeFloat8);

    expectInverse(new DateTime.utc(2016, 10, 1), PostgreSQLCodec.TypeDate);
    expectInverse(new DateTime.utc(1920, 10, 1), PostgreSQLCodec.TypeDate);
    expectInverse(new DateTime.utc(2120, 10, 5), PostgreSQLCodec.TypeDate);

    expectInverse(new DateTime.utc(1920, 10, 1), PostgreSQLCodec.TypeTimestamp);
    expectInverse(new DateTime.utc(2120, 10, 5), PostgreSQLCodec.TypeTimestamp);

    expectInverse(
        new DateTime.utc(1920, 10, 1), PostgreSQLCodec.TypeTimestampTZ);
    expectInverse(
        new DateTime.utc(2120, 10, 5), PostgreSQLCodec.TypeTimestampTZ);
  });

  test("Escape strings", () {
    //                                                       '   b   o    b   '
    expect(UTF8.encode(PostgreSQLCodec.encode('bob')),
        equals([39, 98, 111, 98, 39]));

    //                                                         '   b   o   \n   b   '
    expect(UTF8.encode(PostgreSQLCodec.encode('bo\nb')),
        equals([39, 98, 111, 10, 98, 39]));

    //                                                         '   b   o   \r   b   '
    expect(UTF8.encode(PostgreSQLCodec.encode('bo\rb')),
        equals([39, 98, 111, 13, 98, 39]));

    //                                                         '   b   o  \b   b   '
    expect(UTF8.encode(PostgreSQLCodec.encode('bo\bb')),
        equals([39, 98, 111, 8, 98, 39]));

    //                                                     '   '   '   '
    expect(UTF8.encode(PostgreSQLCodec.encode("'")), equals([39, 39, 39, 39]));

    //                                                      '   '   '   '   '   '
    expect(UTF8.encode(PostgreSQLCodec.encode("''")),
        equals([39, 39, 39, 39, 39, 39]));

    //                                                       '   '   '   '   '   '
    expect(UTF8.encode(PostgreSQLCodec.encode("\''")),
        equals([39, 39, 39, 39, 39, 39]));

    //                                                       sp   E   '   \   \   '   '   '   '   '
    expect(UTF8.encode(PostgreSQLCodec.encode("\\''")),
        equals([32, 69, 39, 92, 92, 39, 39, 39, 39, 39]));

    //                                                      sp   E   '   \   \   '   '   '
    expect(UTF8.encode(PostgreSQLCodec.encode("\\'")),
        equals([32, 69, 39, 92, 92, 39, 39, 39]));
  });

  test("Encode DateTime", () {
    // Get users current timezone
    var tz = new DateTime(2001, 2, 3).timeZoneOffset;
    var tzOffsetDelimiter = "${tz.isNegative ? '-' : '+'}"
        "${tz.abs().inHours.toString().padLeft(2, '0')}"
        ":${(tz.inSeconds % 60).toString().padLeft(2, '0')}";

    var pairs = {
      "2001-02-03T00:00:00.000$tzOffsetDelimiter":
          new DateTime(2001, DateTime.FEBRUARY, 3),
      "2001-02-03T04:05:06.000$tzOffsetDelimiter":
          new DateTime(2001, DateTime.FEBRUARY, 3, 4, 5, 6, 0),
      "2001-02-03T04:05:06.999$tzOffsetDelimiter":
          new DateTime(2001, DateTime.FEBRUARY, 3, 4, 5, 6, 999),
      "0010-02-03T04:05:06.123$tzOffsetDelimiter BC":
          new DateTime(-10, DateTime.FEBRUARY, 3, 4, 5, 6, 123),
      "0010-02-03T04:05:06.000$tzOffsetDelimiter BC":
          new DateTime(-10, DateTime.FEBRUARY, 3, 4, 5, 6, 0),
      "012345-02-03T04:05:06.000$tzOffsetDelimiter BC":
          new DateTime(-12345, DateTime.FEBRUARY, 3, 4, 5, 6, 0),
      "012345-02-03T04:05:06.000$tzOffsetDelimiter":
          new DateTime(12345, DateTime.FEBRUARY, 3, 4, 5, 6, 0)
    };

    pairs.forEach((k, v) {
      expect(PostgreSQLCodec.encode(v), "'$k'");
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

    pairs.forEach((k, v) {
      expect(PostgreSQLCodec.encode(v), "$k");
      expect(
          PostgreSQLCodec.encode(v, dataType: PostgreSQLDataType.real), "$k");
      expect(
          PostgreSQLCodec.encode(v, dataType: PostgreSQLDataType.double), "$k");
    });

    expect(PostgreSQLCodec.encode(1, dataType: PostgreSQLDataType.double), "1");

    expect(PostgreSQLCodec.encode(null, dataType: PostgreSQLDataType.real),
        "null");
    expect(PostgreSQLCodec.encode(null, dataType: PostgreSQLDataType.double),
        "null");
  });

  test("Encode Int", () {
    expect(
        PostgreSQLCodec.encode(1.0, dataType: PostgreSQLDataType.integer), "1");

    expect(PostgreSQLCodec.encode(1), "1");
    expect(
        PostgreSQLCodec.encode(1, dataType: PostgreSQLDataType.integer), "1");
    expect(PostgreSQLCodec.encode(1, dataType: PostgreSQLDataType.bigInteger),
        "1");
    expect(PostgreSQLCodec.encode(1, dataType: PostgreSQLDataType.smallInteger),
        "1");

    expect(PostgreSQLCodec.encode(null, dataType: PostgreSQLDataType.integer),
        "null");
    expect(
        PostgreSQLCodec.encode(null, dataType: PostgreSQLDataType.bigInteger),
        "null");
    expect(
        PostgreSQLCodec.encode(null, dataType: PostgreSQLDataType.smallInteger),
        "null");
  });

  test("Encode Bool", () {
    expect(PostgreSQLCodec.encode(null, dataType: PostgreSQLDataType.boolean),
        "null");
    expect(PostgreSQLCodec.encode(true), "TRUE");
    expect(PostgreSQLCodec.encode(false), "FALSE");
    expect(PostgreSQLCodec.encode(true, dataType: PostgreSQLDataType.boolean),
        "TRUE");
    expect(PostgreSQLCodec.encode(false, dataType: PostgreSQLDataType.boolean),
        "FALSE");
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

expectInverse(dynamic value, int dataType) {
  var encodedValue = PostgreSQLCodec.encodeBinary(value, dataType);
  var decodedValue = PostgreSQLCodec.decodeValue(
      new ByteData.view(encodedValue.buffer), dataType);
  expect(decodedValue, value);
}
