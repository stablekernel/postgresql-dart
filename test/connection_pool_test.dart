import 'dart:async';

import 'package:postgres/postgres.dart';
import 'package:test/test.dart';

void main() {
  group("Connection pool", () {
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

    test("Empty test", () async {
    });

    test("One connection", () async {
      expect(await pool.connection.execute("select 1"), equals(1));
    });

    test("Recreation", () async {
      for(int i=0; i < 5; i++ ) {
        var connection = pool.connection;
        expect(await connection.execute("select 1"), equals(1));
        await connection.close();
      }
      await new Future.delayed(new Duration(milliseconds: 200));
      for(int i=0; i < 5; i++ ) {
        var connection = pool.connection;
        expect(await connection.execute("select 1"), equals(1));
        await connection.close();
      }
    });
    test("Many quaryes", () async {
      var queryes = new List<Future>();
      for(int i=0; i < 20; i++ ) {
        var connection = pool.connection;
        queryes.add(connection.query("select $i"));
      }
      await Future.wait(queryes);
      for(int i=0; i < 20; i++ ) {
        var result = await queryes[i];
        expect(result.first.first, i);
      }
    });
    test("Not available connections", () async {
      for(int i=0; i < 5; i++ ) {
        var connection = pool.connection;
        expect(await connection.execute("select 1"), equals(1));
        await connection.close();
      }
      try {
        var connection = pool.connection;
        expect(true, false);
      }
      on PostgreSQLException catch(e) {
        expect(e.message, contains("not available connections"));
      }
    });
    test("Get connection after close", () async {
      await pool.close();
      try {
        var connection = pool.connection;
        expect(true, false);
      }
      on PostgreSQLException catch(e) {
        expect(e.message, contains("but pool is closed"));
      }
    });
  });
}
