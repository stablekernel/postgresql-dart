import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

const String WKT_POINT = 'POINT ( 10 10)';

const String WKT_LINESTRING = 'LINESTRING (10 10, 20 20, 30 40)';

const String WKT_LINEARRING = 'LINEARRING (10 10, 20 20, 30 40, 10 10)';

const String WKT_POLY = 'POLYGON ((50 50, 50 150, 150 150, 150 50, 50 50))';

const String WKT_MULTIPOINT = 'MULTIPOINT ((10 10), (20 20))';

const String WKT_MULTILINESTRING =
    'MULTILINESTRING ((10 10, 20 20), (15 15, 30 15))';

const String WKT_MULTIPOLYGON =
    'MULTIPOLYGON (((10 10, 10 20, 20 20, 20 15, 10 10)), ((60 60, 70 70, 80 60, 60 60)))';

const String WKT_GC =
    'GEOMETRYCOLLECTION (POLYGON ((100 200, 200 200, 200 100, 100 100, 100 200)), LINESTRING (150 250, 250 250))';

const String multiInsert = '''
    INSERT INTO test(geom) values (GeomFromEWKT('SRID=4326;POINT(0 0)'));
    INSERT INTO test(geom) values (GeomFromEWKT('SRID=4326;POINT(-2 2)'));
    INSERT INTO test(geom) values (GeomFromEWKT('SRID=4326;MULTIPOINT(2 1,1 2)'));
    INSERT INTO test(geom) values (GeomFromEWKT('SRID=4326;LINESTRING(0 0,1 1,1 2)'));
    INSERT INTO test(geom) values (GeomFromEWKT('SRID=4326;MULTILINESTRING((1 0,0 1,3 2),(3 2,5 4))'));
    INSERT INTO test(geom) values (GeomFromEWKT('SRID=4326;POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1, 2 1, 2 2, 1 2,1 1))'));
    INSERT INTO test(geom) values (GeomFromEWKT('SRID=4326;MULTIPOLYGON(((1 1,3 1,3 3,1 3,1 1),(1 1,2 1,2 2,1 2,1 1)), ((-1 -1,-1 -2,-2 -2,-2 -1,-1 -1)))'));
    INSERT INTO test(geom) values (GeomFromEWKT('SRID=4326;GEOMETRYCOLLECTION(POLYGON((1 1, 2 1, 2 2, 1 2,1 1)),POINT(2 3),LINESTRING(2 3,3 4))'));
  ''';

void main() {
  PostgreSQLConnection connection;

  final geomFactory = GeometryFactory.withCoordinateSequenceFactory(
      PackedCoordinateSequenceFactory.withType(
          PackedCoordinateSequenceFactory.DOUBLE));
  final rdr = WKTReader.withFactory(geomFactory);

  setUp(() async {
    connection = PostgreSQLConnection('localhost', 5432, 'alaska',
        username: 'dart', password: 'dart');
    await connection.open();

    await connection.execute('''
        DROP TABLE IF EXISTS test;
        CREATE EXTENSION IF NOT EXISTS postgis;
        CREATE TABLE IF NOT EXISTS test(gid serial PRIMARY KEY, geom geometry);
    ''');
  });

  tearDown(() async {
    await connection?.close();
  });

  group('Storage', () {
    test('Can store point and read point as dart_jts.Point', () async {
      final result = await connection.query(
        'INSERT into test(geom) VALUES (@point) returning geom',
        substitutionValues: {'point': WKT_POINT},
      );
      final geom = result[0][0] as Point;
      // print(geom.toString());

      expect(geom.equalsExactGeom(rdr.read(WKT_POINT)), true);
    });

    test('Can store linestring and read linestring as dart_jts.LineString',
        () async {
      final result = await connection.query(
        'INSERT into test(geom) VALUES (@linestring) returning geom',
        substitutionValues: {'linestring': WKT_LINESTRING},
      );

      final geom = result[0][0] as LineString;
      // print(geom.toString());
      final lineString = rdr.read(WKT_LINESTRING);

      expect(geom.toString(), lineString.toString());
      expect(geom.SRID, lineString.SRID);
      expect(geom.envelope, lineString.envelope);
      expect(geom.equals(lineString), true);

      expect(geom.toText(), lineString.toText());
    });

    // test('Can store linearring and read linearring as dart_jts.LinearRing',
    //     () async {
    //   final result = await connection.query(
    //     'INSERT into test(geom) VALUES (@linearRing) returning ST_IsValid(geom)',
    //     substitutionValues: {'linearRing': rdr.read(WKT_LINEARRING).toText()},
    //   );
    //   // final geom = result[0][0] as LinearRing;
    //   // final linearRing = rdr.read(WKT_LINEARRING);

    //   // expect(geom.equalsExactGeom(linearRing), true);
    //   // expect(geom.SRID, linearRing.SRID);
    //   // expect(geom.toText(), linearRing.toText());
    //   expect(result[0][0], false);
    // });

    test('Can store polygon and read it as dart_jts.Polygon', () async {
      final result = await connection.query(
        'INSERT into test(geom) VALUES (@polygon) returning geom',
        substitutionValues: {'polygon': WKT_POLY},
      );

      final geom = result[0][0] as Polygon;
      final poly = rdr.read(WKT_POLY);

      expect(geom.equalsExactGeom(poly), true);
      expect(geom.SRID, poly.SRID);
      expect(geom.toText(), poly.toText());
    });

    test('Can store MultiPoint and read it as dart_jts.MultiPolygon', () async {
      final result = await connection.query(
        'INSERT into test(geom) VALUES (@multiPoint) returning geom',
        substitutionValues: {'multiPoint': WKT_MULTIPOINT},
      );

      final geom = result[0][0] as MultiPoint;
      final multiPoint = rdr.read(WKT_MULTIPOINT);

      expect(geom.equalsExactGeom(multiPoint), true);
      expect(geom.SRID, multiPoint.SRID);
      expect(geom.toText(), multiPoint.toText());
    });

    test(
        'Can store MultiLineString well and read it as dart_jts.MultiLineString',
        () async {
      final result = await connection.query(
        'INSERT into test(geom) VALUES (@multiLine),(@multiLine) returning geom,geom',
        substitutionValues: {'multiLine': WKT_MULTILINESTRING},
      );

      final geom = result[0][0] as MultiLineString;
      final geom1 = result[0][1] as MultiLineString;
      final multiLineString = rdr.read(WKT_MULTILINESTRING);

      expect(geom.equals(geom1), true);

      expect(geom.equalsExactGeom(multiLineString), true);
      expect(geom.SRID, multiLineString.SRID);
      expect(geom.toText(), multiLineString.toText());
    });

    test('Can store multipolygon and read it as dart_jts.MultiPolgon',
        () async {
      final result = await connection.query(
        'INSERT into test(geom) VALUES (@multiPoly) returning geom',
        substitutionValues: {'multiPoly': WKT_MULTIPOLYGON},
      );
      final geom = result[0][0] as MultiPolygon;
      final actualGeom = rdr.read(WKT_MULTIPOLYGON);

      expect(actualGeom.equals(geom), true);
      expect(geom.SRID, actualGeom.SRID);
      expect(geom.toText(), actualGeom.toText());
    });

    test('Can store GeometryCollection well', () async {
      final result = await connection.query(
        'INSERT into test(geom) VALUES (@geomColl) returning geom',
        substitutionValues: {'geomColl': WKT_GC},
      );

      final geom = result[0][0] as GeometryCollection;
      // final actualGeom = rdr.read(WKT_GC); //TODO: Issue with wkt reading GeometryCollections

      // expect(actualGeom.equals(geom), true);
      // expect(geom.SRID, actualGeom.SRID);
      expect(geom.toText(), WKT_GC);
    });

    test(
        'MultiInsert should return appropriate inserted geometries when read back',
        () async {
      final sql = '''
        INSERT INTO test(geom) values 
        (GeomFromEWKT('SRID=4326;POINT(0 0)')),
        (GeomFromEWKT('SRID=4326;POINT(-2 2)')),
        (GeomFromEWKT('SRID=4326;MULTIPOINT(2 1,1 2)')),
        (GeomFromEWKT('SRID=4326;LINESTRING(0 0,1 1,1 2)')),
        (GeomFromEWKT('SRID=4326;MULTILINESTRING((1 0,0 1,3 2),(3 2,5 4))')),
        (GeomFromEWKT('SRID=4326;POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1, 2 1, 2 2, 1 2,1 1))')),
        (GeomFromEWKT('SRID=4326;MULTIPOLYGON(((1 1,3 1,3 3,1 3,1 1),(1 1,2 1,2 2,1 2,1 1)), ((-1 -1,-1 -2,-2 -2,-2 -1,-1 -1)))')),
        (GeomFromEWKT('SRID=4326;GEOMETRYCOLLECTION(POLYGON((1 1, 2 1, 2 2, 1 2,1 1)),POINT(2 3),LINESTRING(2 3,3 4))'))
        RETURNING geom,geom,geom,geom,geom,geom,geom,geom
      ''';
      final results = await connection.query(sql);
      final point = results[0][0] as Point;
      final point2 = results[1][0] as Point;
      final multiPoint = results[2][0] as MultiPoint;
      final lineString = results[3][0] as LineString;
      final multiLineString = results[4][0] as MultiLineString;
      final polygon = results[5][0] as Polygon;
      final multiPolygon = results[6][0] as MultiPolygon;
      final geomCollection = results[7][0] as GeometryCollection;

      expect(point.SRID, 4326);
      expect(point.coordinates.getX(0), 0);
      expect(point.coordinates.getY(0), 0);

      expect(point2.SRID, 4326);
      expect(point2.coordinates.getX(0), -2);
      expect(point2.coordinates.getY(0), 2);

      expect(multiPoint.getCoordinates().first, Coordinate(2, 1));
      expect(multiPoint.getCoordinates().elementAt(1), Coordinate(1, 2));

      expect(lineString.getCoordinates().length, 3);
      expect(lineString.getCoordinates().elementAt(0), Coordinate(0, 0));
      expect(lineString.getCoordinates().elementAt(1), Coordinate(1, 1));
      expect(lineString.getCoordinates().elementAt(2), Coordinate(1, 2));
      expect(lineString.SRID, 4326);

      expect(multiLineString.SRID, 4326);
      expect(multiLineString.getNumGeometries(), 2);
      expect(
        multiLineString.getGeometryN(0).equals(
              geomFactory.createLineString(
                [
                  Coordinate(1, 0),
                  Coordinate(0, 1),
                  Coordinate(3, 2),
                ],
              ),
            ),
        true,
      );
      expect(
        multiLineString.getGeometryN(1).equals(
              geomFactory.createLineString(
                [
                  Coordinate(3, 2),
                  Coordinate(5, 4),
                ],
              ),
            ),
        true,
      );

      expect(polygon.SRID, 4326);
      expect(polygon.getNumInteriorRing(), 1);
      expect(
        polygon.getInteriorRingN(0).equals(
              geomFactory.createLinearRing(
                [
                  Coordinate(1, 1),
                  Coordinate(2, 1),
                  Coordinate(2, 2),
                  Coordinate(1, 2),
                  Coordinate(1, 1),
                ],
              ),
            ),
        true,
      );

      expect(
          polygon.getExteriorRing().equals(
                geomFactory.createLinearRing(
                  [
                    Coordinate(0, 0),
                    Coordinate(4, 0),
                    Coordinate(4, 4),
                    Coordinate(0, 4),
                    Coordinate(0, 0)
                  ],
                ),
              ),
          true);

      expect(multiPolygon.getNumGeometries(), 2);
      expect(multiPolygon.SRID, 4326);
      final polygon1 = multiPolygon.getGeometryN(0) as Polygon;
      final polygon2 = multiPolygon.getGeometryN(1) as Polygon;

      expect(polygon1.getGeometryType(), 'Polygon');
      expect(polygon2.getGeometryType(), 'Polygon');

      expect(polygon1.getNumInteriorRing(), 1);
      expect(
        polygon1.getInteriorRingN(0).equals(
              geomFactory.createLinearRing(
                [
                  //1 1,2 1,2 2,1 2,1 1
                  Coordinate(1, 1),
                  Coordinate(2, 1),
                  Coordinate(2, 2),
                  Coordinate(1, 2),
                  Coordinate(1, 1)
                ],
              ),
            ),
        true,
      );

      expect(
        polygon1.getExteriorRing().equals(
              geomFactory.createLinearRing(
                [
                  //1 1,3 1,3 3,1 3,1 1
                  Coordinate(1, 1),
                  Coordinate(3, 1),
                  Coordinate(3, 3),
                  Coordinate(1, 3),
                  Coordinate(1, 1)
                ],
              ),
            ),
        true,
      );

      expect(
        polygon2.getExteriorRing().equals(
              geomFactory.createLinearRing(
                [
                  //-1 -1,-1 -2,-2 -2,-2 -1,-1 -1
                  Coordinate(-1, -1),
                  Coordinate(-1, -2),
                  Coordinate(-2, -2),
                  Coordinate(-2, -1),
                  Coordinate(-1, -1)
                ],
              ),
            ),
        true,
      );

      expect(geomCollection.SRID, 4326);
      expect(geomCollection.getNumGeometries(), 3);

      final polygonInGC = geomCollection.getGeometryN(0) as Polygon;
      final pointInGC = geomCollection.getGeometryN(1) as Point;
      final lineStringInGC = geomCollection.getGeometryN(2) as LineString;

      expect(
        polygonInGC.getExteriorRing().equals(
              geomFactory.createLinearRing(
                [
                  Coordinate(1, 1),
                  Coordinate(2, 1),
                  Coordinate(2, 2),
                  Coordinate(1, 2),
                  Coordinate(1, 1)
                ],
              ),
            ),
        true,
      );

      expect(pointInGC.getCoordinate().getX(), 2);
      expect(pointInGC.getCoordinate().getY(), 3);

      expect(
        lineStringInGC.equals(
          geomFactory.createLineString(
            [
              Coordinate(2, 3),
              Coordinate(3, 4),
            ],
          ),
        ),
        true,
      );
    });
  });
}
