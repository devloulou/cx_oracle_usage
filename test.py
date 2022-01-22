import cx_Oracle as ora
from datetime import datetime

ora.init_oracle_client(lib_dir=r"C:\oracle\instantclient_21_3")

username = "RICSI"
password = "Python_12345678"
tns_alias = "pytstdb_high"

# connection
# cursor
# minden művelet a cursor objektumon keresztul történik
# connection-on keresztl megy a tranzakció kezelés
# mindig le kell zárni a cursort meg a connection-t

conn = ora.connect(user=username, password=password, dsn=tns_alias, encoding="UTF-8")
cursor = conn.cursor()

#cursor.execute("select sysdate from dual")

#print(cursor.fetchone()[0])

out_val = cursor.var(str)
out_val = cursor.var(ora.DB_TYPE_VARCHAR)

cursor.execute("""
begin
    get_day_name_pcd(:1,:2);
end;

""", [datetime.strptime('2001.09.11.', '%Y.%m.%d.'), out_val])

print(out_val.getvalue())

out_val = cursor.var(ora.DB_TYPE_VARCHAR)

#cursor.callproc("get_day_name_pcd", [datetime.strptime('2001.09.11.', '%Y.%m.%d.'), out_val])
#cursor.callproc("get_day_name_pcd", [123, out_val])

#cursor.callfunc()

#print(out_val.getvalue())


return_val = cursor.callfunc("myfunc", int, ["a string", 15])

print(return_val)     