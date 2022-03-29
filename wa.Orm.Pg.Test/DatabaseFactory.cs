using System;
using System.Threading.Tasks;
using wa.Orm.Pg.Test.Models;
using Npgsql;

namespace wa.Orm.Pg.Test
{
    public class DatabaseFactory
    {
        public static NpgsqlConnection Connect(bool mapEnums = true)
        {
            var conn = new NpgsqlConnection("Host=localhost;Port=5432;Database=pgormtest;User ID=postgres;Password=postgres;");
            conn.Open();
            if (mapEnums)
            {
                conn.TypeMapper.MapEnum<Gender>("gender");
            }
            int timeZoneOffset = TimeZoneInfo.Local.GetUtcOffset(DateTime.UtcNow).Hours;
            conn.Execute($"SET TIME ZONE {timeZoneOffset}");

            return conn;
        }
        public static async Task<NpgsqlConnection> ConnectAsync(bool mapEnums = true)
        {
            var conn = new NpgsqlConnection("Host=localhost;Port=5432;Database=pgormtest;User ID=postgres;Password=postgres;");
            await conn.OpenAsync().ConfigureAwait(false);
            if (mapEnums)
            {
                conn.TypeMapper.MapEnum<Gender>("gender");
            }
            int timeZoneOffset = TimeZoneInfo.Local.GetUtcOffset(DateTime.UtcNow).Hours;
            await conn.ExecuteAsync($"SET TIME ZONE {timeZoneOffset}").ConfigureAwait(false);

            return conn;
        }

        public static void CreatePostgres()
        {
            using (var connection = Connect(false))
            {

                if (!connection.Scalar<bool?>("SELECT true FROM pg_type WHERE typname = 'gender'").GetValueOrDefault())
                {
                    connection.Execute(@"CREATE TYPE gender AS ENUM ('unknown', 'male', 'female');");
                    connection.ReloadTypes();
                }

                connection.Execute(@"
                    CREATE TABLE IF NOT EXISTS persons
                    (
                      id serial,
                      first_name text,
                      last_name text,
                      age integer,
                      gender gender,
                      date_created timestamptz DEFAULT NOW(),
                      CONSTRAINT persons_pk PRIMARY KEY (id)
                    )");

                connection.Execute(
                    "TRUNCATE TABLE persons CASCADE");

                connection.Execute(@"
                    CREATE TABLE IF NOT EXISTS document
                    (
                      id uuid NOT NULL,
                      name text,
                      data bytea,
                      date_created timestamptz,
                      CONSTRAINT document_pk PRIMARY KEY (id)
                    )");

                connection.Execute(
                    "TRUNCATE TABLE document CASCADE");

                connection.Execute(@"
                    CREATE TABLE IF NOT EXISTS cars
                    (
                      id text,
                      make text,
                      date_registered timestamptz DEFAULT NOW(),
                      CONSTRAINT cars_pk PRIMARY KEY (id)
                    )");

                connection.Execute(
                    "TRUNCATE TABLE cars CASCADE");
            }
        }
    }
}
