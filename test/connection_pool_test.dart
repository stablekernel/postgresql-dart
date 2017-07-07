import 'dart:async';

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
      expect(await pool.connection.execute("select 1"), equals(1));
    });

    test("Pool connection recreation", () async {
      for(int i=0; i < 20; i++ ) {
        var connection = pool.connection;
        expect(await connection.execute("select 1"), equals(1));
        await connection.close();
      }
    });
    test("Pool connection many quaryes", () async {
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
  });
}
