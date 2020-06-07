import 'package:dart_jts/dart_jts.dart';

abstract class PostgisGeometryParser {
  Geometry parseGeometry(List<int> ewkt);
  Point parsePoint(List<int> ewkt);
  LineString parseLineString(List<int> ewkt);
  Polygon parsePolygon(List<int> ewkt);

  MultiPoint parseMultiPoint(List<int> ewkt);
  MultiLineString parseMultiLineString(List<int> ewkt);
  MultiPolygon parseMultiPolygon(List<int> ewkt);
  GeometryCollection parseGeometryCollection(List<int> ewkt);
  MultiLineString readMultiCurve(List<int> ewkt);
}



class EwkbFormatException implements Exception {
  final String message;

  EwkbFormatException(this.message) : super();

  @override
  String toString() {
    return 'FormatConversionException($message)';    
  }

}


class PostgisEWKTParser extends PostgisGeometryParser {

  var wkbReader = WKBReader();

  @override
  LineString parseLineString(List<int> ewkt) {
    final lineString = wkbReader.read(ewkt);
    if(lineString is LineString) {
      return lineString;
    } else {
      throw EwkbFormatException('$ewkt is not valid LineString. Ensure your format is EWKB & is a LineString');
    }
  }

  @override
  MultiPolygon parseMultiPolygon(List<int> ewkt) {
    final multiPolygon = wkbReader.read(ewkt);
    if(multiPolygon is MultiPolygon) {
      return multiPolygon;
    } else {
      throw EwkbFormatException('$ewkt is not valid MultiPolygon. Ensure your format is EWKB & is a MultiPolygon');
    }
  }

  @override
  Point parsePoint(List<int> ewkt) {
    final point = wkbReader.read(ewkt);
    if(point is Point) {
      return point;
    } else {
      throw EwkbFormatException('$ewkt is not valid Point. Ensure your format is EWKB & is a Point');
    }
  }

  @override
  Polygon parsePolygon(List<int> ewkt) {
    final point = wkbReader.read(ewkt);
    if(point is Polygon) {
      return point;
    } else {
      throw EwkbFormatException('$ewkt is not valid Polygon. Ensure your format is EWKB & is a Polygon');
    }
  }

  @override
  GeometryCollection parseGeometryCollection(List<int> ewkt) {
    final geometryCollection = wkbReader.read(ewkt);
    if(geometryCollection is GeometryCollection) {
      return geometryCollection;
    } else {
      throw EwkbFormatException('$ewkt is not valid GeometryCollection. Ensure your format is EWKB & is a GeometryCollection');
    }
  }
  
  @override
  Geometry parseGeometry(List<int> ewkt) {
    final geometry = wkbReader.read(ewkt);
    if(geometry is Geometry) {
      return geometry;
    } else {
      throw EwkbFormatException('$ewkt is not valid Geometry. Ensure your format is EWKB & is a Geometry');
    }
  }
  
    @override
    MultiLineString parseMultiLineString(List<int> ewkt) {
      final multiLineString = wkbReader.read(ewkt);
      if(multiLineString is MultiLineString) {
        return multiLineString;
      } else {
        throw EwkbFormatException('$ewkt is not valid MultiLineString. Ensure your format is EWKB & is a MultiLineString');
      }
    }
  
    @override
    MultiPoint parseMultiPoint(List<int> ewkt) {
      final multiPoint = wkbReader.read(ewkt);
      if(multiPoint is MultiPoint) {
        return multiPoint;
      } else {
        throw EwkbFormatException('$ewkt is not valid MultiPoint. Ensure your format is EWKB & is a MultiPoint');
      }
    }

  @override
  MultiLineString readMultiCurve(List<int> ewkt) {
    final multiCurve = wkbReader.read(ewkt);
    if(multiCurve is MultiLineString) {
      return multiCurve;
    } else {
      throw EwkbFormatException('$ewkt is not valid MultiPoint. Ensure your format is EWKB & is a MultiPoint');
    }
  }
}