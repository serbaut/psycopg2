import datetime
import decimal
import os
import psycopg2
from testutils import unittest
from testconfig import dsn

# create user psycopg2;
# grant all on schema public to psycopg2;

class VerticaTests(unittest.TestCase):

    def setUp(self):
        dbhost = os.environ.get('PSYCOPG2_TESTVERTICA_HOST', 'localhost')
        dbport = os.environ.get('PSYCOPG2_TESTVERTICA_PORT', '5433')
        dbuser = os.environ.get('PSYCOPG2_TESTVERTICA_USER', 'psycopg2')
        dbpass = os.environ.get('PSYCOPG2_TESTVERTICA_PASSWORD', None)
        try:
            self.conn = psycopg2.connect(host=dbhost, user=dbuser, password=dbpass, port=dbport)
        except psycopg2.OperationalError, e:
            self.conn = None

    def test_return_types(self):
        if not self.conn:
            return self.skipTest("vertica not available")

        types = [("integer", 4711, 4711L),
                 ("bigint", 4711, 4711L),
                 ("tinyint", 4711, 4711L),
                 ("double precision", 4711.0),
                 ("float", 4711.0),
                 ("char(10)", u"foo       "),
                 ("varchar(10)", u"foo"),
                 ("money", decimal.Decimal("4711.0")),
                 ("decimal(10,1)", decimal.Decimal("4711.0")),
                 ("bool", True),
                 ("date", datetime.date(2001,1,1)),
                 ("datetime", datetime.datetime(2001, 1, 1, 23, 59, 59)),
                 ("time", datetime.time(12,24)),
                 ("timestamp", datetime.datetime(2001, 1, 1, 23, 59, 59)),
                 ("interval", datetime.timedelta(seconds=10)),
                 ]

        curs = self.conn.cursor()
        for t in types:
            sql_type, val = t[:2]
            want = t[2] if len(t) > 2 else t[1]
            curs.execute("drop table if exists psycopg2")
            curs.execute("create table psycopg2 (x %s)" % sql_type)
            curs.execute("insert into psycopg2 values (%s)", (val,))
            curs.execute("select * from psycopg2")
            got, = curs.fetchone()
            self.assertEqual(got, want)
            self.assertEqual(type(got), type(want))

        curs.execute("drop table psycopg2")

def test_suite():
    return unittest.TestLoader().loadTestsFromName(__name__)

if __name__ == "__main__":
    unittest.main()
