import base64
import getopt
import os
import sys

def main(argv):
    reqargs = 5
    outfile = None
    conn_replace = None
    try:
        opts, args = getopt.getopt(argv, "ho:r", ["help", "outfile=", "replace"])
        for opt, val in opts:
            if opt in ("-h", "--help"):
                usage()
                sys.exit()
            elif opt in ("-o", "--outfile"):
                outfile = val
            elif opt in ("-r", "--replace"):
                conn_replace = True
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    if len(args) != reqargs:
        print("Incorrect number of arguments: {} given, {} required.".format(len(args), reqargs))
        usage()
        sys.exit(2)
    conn_name = args[0]
    krb5_user = args[1]
    krb5_config = args[2]
    krb5_keytab = args[3]
    jdbc_connection_string = args[4]
    stmt = getcreateconn(conn_name, conn_replace, krb5_user, krb5_config, krb5_keytab, jdbc_connection_string)
    if outfile:
        appendtofile(outfile, stmt + "\n")
    else:
        print(stmt)

def getcreateconn(name, replace, user, conf, keytab, jdbc_connection_string):
    krb5_conf_b64 = getbase64(conf)
    krb5_keytab_b64 = getbase64(keytab)
    conn = "CREATE "
    if replace:
        conn += "OR REPLACE "
    conn += "CONNECTION {} TO '{}' ".format(name, jdbc_connection_string)
    conn += "USER '{}' ".format(user)
    conn += "IDENTIFIED BY 'ExaAuthType=Kerberos;{};{}'".format(krb5_conf_b64, krb5_keytab_b64)
    return conn

def getbase64(path):
    if path and os.path.isfile(path):
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    else:
        print("File does not exist: ", path)
        sys.exit(0)

def appendtofile(path, data):
    with open(path, "a") as f:
        f.write(data)

def usage():
    txt  = "Generate a CREATE CONNECTION SQL statement to be used for Kerberos\n"
    txt += "authentication. The statement can be executed directly in EXASOL\n"
    txt += "to create the CONNECTION.\n"
    txt += "\nUsage:\n"
    txt += "  python {} [option] [connection] [principal] [config] [keytab] [jdbc_connection_string]\n".format(sys.argv[0])
    txt += "\nOptions:\n"
    txt += "  -h, --help   : print usage and exit\n"
    txt += "  -o, --outfile: append output to the given file\n"
    txt += "  -r, --replace: add 'OR REPLACE' option to 'CREATE CONNECTION' statement\n"
    txt += "\nArguments:\n"
    txt += "  connection             : CONNECTION name\n"
    txt += "  principal              : Kerberos principal\n"
    txt += "  config                 : Kerberos configuration file path\n"
    txt += "  keytab                 : Kerberos keytab path\n"
    txt += "  jdbc_connection_string : JDBC connection string\n"
    txt += "\nExamples:\n"
    txt += "  python {} krb_conn user@EXAMPLE.COM /etc/krb5.conf user.keytab \\\n".format(sys.argv[0])
    txt += "    'jdbc:hive2://hive-host.example.com:10000;AuthMech=1;KrbRealm=EXAMPLE.COM;KrbHostFQDN=hive-host.example.com;KrbServiceName=hive'\n"
    txt += "    =>  CREATE CONNECTION krb_conn'\n"
    txt += "        TO 'jdbc:hive2://hive-host.example.com:10000;AuthMech=1;KrbRealm=EXAMPLE.COM;KrbHostFQDN=hive-host.example.com;KrbServiceName=hive'\n"
    txt += "        USER 'user@EXAMPLE.COM'\n"
    txt += "        IDENTIFIED BY 'ExaAuthType=Kerberos;enp6Cg==;YWFhCg=='\n"
    txt += "\n  python {} -r -o out.txt krb_conn user@EXAMPLE.COM /etc/krb5.conf user.keytab \\\n".format(sys.argv[0])
    txt += "    'jdbc:hive2://hive-host.example.com:10000;AuthMech=1;KrbRealm=EXAMPLE.COM;KrbHostFQDN=hive-host.example.com;KrbServiceName=hive'\n"
    txt += "    =>  CREATE OR REPLACE CONNECTION krb_conn\n"
    txt += "        TO ''\n"
    txt += "        USER 'user@EXAMPLE.COM' IDENTIFIED BY 'ExaAuthType=Kerberos;enp6Cg==;YWFhCg==' (written to out.txt)\n"
    print(txt)

if __name__ == "__main__":
    main(sys.argv[1:])
