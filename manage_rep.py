# -*- coding: UTF-8 -*-
# 实现功能：
#  1. Repmgr 安装 && 配置
#  2. Repmgr 备库搭建
#  3. Repmgr witness 安装 && 配置
#  4. Repmgr switchover
#  5. Repmgr repmgrd 搭建 && 配置
#  6. Repmgr failover
#  7. Repmgr 失败节点重新加入集群
#  8. Repmgr 更新升级

import os_login
import os
import time

dict={}   #存放用户配置参数
chk = 0


# 用户需要功能参数：Repmgr版本、主备库连接串、备库数据文件目录、witness IP、


class db_install:
    def __init__(self,osip,osuser,osport,dbuser,dbport,version,db_data):
        self.os_user = osuser
        self.os_port = osport
        self.ip = osip
        self.dbuser = dbuser
        self.dbport = dbport
        self.version = version
        self.db_data = db_data

    def check_dir(self,file):
        cmd = 'ls -d ' + file
        global chk   # 获取用户设置参数
        value_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
        if "".join(value_cmd) == '':
            chk = 1
        else:
            chk = 0

    def check_user(self):
        cmd = 'id -u ' + self.dbuser + ' &>/dev/null || echo $?'
        value_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
        if "".join(value_cmd).strip('\n') == '1':
            chk = 1
        else:
            chk = 0

    def trans_file(self,os_cmd):
        value_cmd = os.system(os_cmd)

class install_pg(db_install):
    def __init__(self,osip,osuser,osport,dbuser,dbport,version,db_data):
        db_install.__init__(self,osip,osuser,osport,dbuser,dbport,version,db_data)
        self.dbhome = '/usr/local/postgre' + self.version
        self.file_name = 'postgresql-' + self.version + '.tar.gz'

    def prepare(self):
        if self.os_user == 'root':
            ##检查用户
            print('开始检查用户，请稍候...')
            self.check_user()
            if chk == 1:
                cmd = 'useradd ' + self.dbuser
                os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
                cmd = 'echo "' + self.dbuser + '|passwd --stdin ' + self.dbuser
                os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
                cmd = 'su - ' + self.dbuser + ' -c "mkdir soft"'
                os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
            else:
                pass

        ###检查目录
            print('开始检查安装目录，请稍候...')
            self.check_dir(self.dbhome)
            if chk == 1:
                cmd = 'mkdir -p ' + self.dbhome
                os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
                cmd = 'chown postgres:postgres ' + self.dbhome
                os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
            else:
                cmd = 'ls ' + self.dbhome + '|wc -l'
                value_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
                if "".join(value_cmd).strip('\n') == '0':
                    pass
                else:
                    print('provide directory ' + self.dbhome + ' is not empty!')
                    exit()


            print('开始检查系统参数文件，请稍候...')
            file_list = ['require_file/sysctl.conf', 'require_file/limits.conf']
            for j in file_list:
                if j == 'require_file/sysctl.conf':
                    p = open(j, 'r')
                    try:
                        for i in p:
                            param = i.split('=').pop(0).strip()
                            cmd = 'cat /etc/sysctl.conf|grep -vE \'^$|^#\'|grep ' + param + '|wc -l'
                            value_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
                            if "".join(value_cmd).strip('\n') == '0':
                                cmd = 'echo \'' + i.strip('\n') + '\'>> /etc/sysctl.conf'
                                os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
                            else:
                                pass
                    finally:
                        p.close()
                else:
                    p = open(j, 'r')
                    try:
                        for i in p:
                            param = i.strip('\n')
                            cmd = 'cat /etc/security/limits.conf|grep -vE \'^$|^#\'|grep ' + param + '|wc -l'
                            value_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
                            if "".join(value_cmd).strip('\n') == '0':
                                cmd = 'echo \'' + i.strip('\n') + '\'>> /etc/security/limits.conf'
                                os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
                            else:
                                pass
                    finally:
                        p.close()
            os_login.ssh(self.ip, self.os_user, self.os_port, 'sysctl -p')

            print('开始设置环境变量')
            cmd = 'cat /home/postgres/.bash_profile|grep PGHOME|wc -l'
            value_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
            if "".join(value_cmd).strip('\n') == '0':
                cmd = 'echo \'export PGHOME=' + self.dbhome + '\' >> /home/' + self.dbuser + '/.bash_profile'
                value_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
                cmd = 'echo \'export PATH=$PGHOME/bin:$PATH\' >> /home/' + self.dbuser + '/.bash_profile'
                value_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
                cmd = 'echo \'export PGPORT=' + self.dbport + '\' >> /home/' + self.dbuser + '/.bash_profile'
                value_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
        else:
            self.check_dir(self.dbhome)
            if chk == 1:
                print(self.dbhome + '目录不存在，请创建目录后重新执行!!!')
                exit()
            else:
                cmd = 'ls ' + self.dbhome + '|wc -l'
                value_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
                if "".join(value_cmd).strip('\n') == '0':
                    pass
                else:
                    print('provide directory ' + self.dbhome + ' is not empty!')
                    exit()

    def put_file(self,file):
        print('开始检查安装文件包，请稍候...')
        # file = '/home/postgres/soft/' + self.file_name
        chk_file = '/home/postgres/soft/' + file
        self.check_dir(chk_file)
        if chk == 1:
            print('文件不存在，尝试上传该文件...')
            cmd = 'scp -P ' + self.os_port + ' require_file/' + file + ' ' + self.os_user + '@' + \
                  self.ip + ':' + chk_file
            self.trans_file(cmd)
            self.check_dir(chk_file)
            if chk == 1:
                print('上传失败...')
                exit()
            else:
                pass
        else:
            pass

        if self.os_user == 'root':
            unzip_file = 'su - postgres' + self.dbuser + ' -c "tar -xvf soft/' + self.file_name + \
                         ' -C /home/' + self.dbuser + '/soft"'
            os_login.ssh(self.ip, self.os_user, self.os_port, unzip_file)
        else:
            unzip_file = 'tar -xvf soft/' + self.file_name + ' -C /home/' + self.dbuser + '/soft'
            os_login.ssh(self.ip, self.os_user, self.os_port, unzip_file)


    def install(self):
        print('开始安装系统软件，请稍候...')
        if self.os_user == 'root':
            require_soft_cmd = 'yum install -y tcl* python-dev*  openssl* pam* readline* libxml* perl-ExtUtils-Embed libxslt*'
            os_login.ssh(self.ip, self.os_user, self.os_port, require_soft_cmd)
            print('开始安装软件,请稍候...')
            if self.version.split('.').pop(0) == '9':
                cmd = 'su - ' + self.dbuser + ' -c "cd /home/' + self.dbuser + '/soft/postgresql-' + self.version + \
                      '&& ./configure --prefix=' + self.dbhome + ' --with-pgport=1923 --with-perl --with-tcl --with-python ' \
                      '--with-openssl --with-pam --without-ldap --with-libxml --with-libxslt --enable-thread-safety ' \
                      '--with-wal-blocksize=8 --with-segsize=4 --with-blocksize=8 --with-wal-segsize=64 > /tmp/pg_install.log' \
                      ' 2>>/tmp/pg_install.log && gmake world >> /tmp/pg_install.log 2>>/tmp/pg_install.log && gmake install-world ' \
                      '>> /tmp/pg_install.log 2>>/tmp/pg_install.log && echo $?"'
                val_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
                if "".join(val_cmd[-1]).strip('\n') == '0':
                    print('软件安装成功！')
                else:
                    print('软件安装失败，请查看日志：/tmp/pg_install.log')

            elif self.version.split('.').pop(0) == '10':
                cmd = 'su - ' + self.dbuser + ' -c "cd /home/' + self.dbuser + '/soft/postgresql-' + self.version + \
                      ' && ./configure --prefix=' + self.dbhome + ' --with-pgport=1923 --with-perl --with-tcl --with-python ' \
                      '--with-openssl --with-pam --without-ldap --with-libxml ' \
                      '--with-libxslt --enable-thread-safety --with-wal-blocksize=8' \
                      ' --with-segsize=4 --with-blocksize=8 --with-wal-segsize=64 > ' \
                      '/tmp/pg_install.log 2>>/tmp/pg_install.log && gmake world ' \
                      '>>/tmp/pg_install.log 2>>/tmp/pg_install.log&& gmake install-world' \
                      '>> /tmp/pg_install.log 2>>/tmp/pg_install.log" && echo $?'
                val_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
                print(val_cmd)
                if "".join(val_cmd[-1]).strip('\n') == '0':
                    print('软件安装成功！')
                else:
                    print('软件安装失败，请查看日志：/tmp/pg_install.log')
            else:
                print('安装的数据库版本暂不支持!')

        else:
            print('开始安装软件,请稍候...')
            if self.version.split('.').pop(0)=='9':
                cmd = 'cd /home/' + self.dbuser + '/soft/postgresql-' + self.version +\
                      '&& ./configure --prefix=' + self.dbhome + ' --with-pgport=1923 --with-perl --with-tcl --with-python ' \
                      '--with-openssl --with-pam --without-ldap --with-libxml --with-libxslt --enable-thread-safety ' \
                      '--with-wal-blocksize=8 --with-segsize=4 --with-blocksize=8 --with-wal-segsize=64 > /tmp/pg_install.log' \
                      ' 2>>/tmp/pg_install.log && make >> /tmp/pg_install.log 2>>/tmp/pg_install.log && make install ' \
                      '>> /tmp/pg_install.log 2>>/tmp/pg_install.log && echo $?'
                val_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
                if "".join(val_cmd[-1]).strip('\n') == '0':
                    print('软件安装成功！')
                else:
                    print('软件安装失败，请查看日志：/tmp/pg_install.log')

            elif self.version.split('.').pop(0)=='10':
                cmd = 'cd /home/' + self.dbuser + '/soft/postgresql-' + self.version + \
                      ' && ./configure --prefix=' + self.dbhome + ' --with-pgport=1923 --with-perl --with-tcl --with-python ' \
                      '--with-openssl --with-pam --without-ldap --with-libxml ' \
                      '--with-libxslt --enable-thread-safety --with-wal-blocksize=8' \
                      ' --with-segsize=4 --with-blocksize=8 --with-wal-segsize=64 > ' \
                      '/tmp/pg_install.log 2>>/tmp/pg_install.log && make ' \
                      '>>/tmp/pg_install.log 2>>/tmp/pg_install.log&& make install' \
                      '>> /tmp/pg_install.log 2>>/tmp/pg_install.log && echo $?'
                val_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
                print(val_cmd)
                if "".join(val_cmd[-1]).strip('\n') == '0':
                    print('软件安装成功！')
                else:
                    print('软件安装失败，请查看日志：/tmp/pg_install.log')
            else:
                print('安装的数据库版本暂不支持!')

    def install_soft(self,version):
        print('开始安装repmgr软件...')
        if self.os_user == 'root':
            cmd = 'su - postgres -c "cd /home/postgres/soft/repmgr-' + version +\
                  ' && ./configure > /tmp/install_repmgr.log' \
                  '&& make install >> /tmp/install_repmgr.log" && echo $?'
            value_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
            if "".join(value_cmd[-1]).strip('\n') == '0':
                print('Repmgr软件安装成功')
            else:
                print('Repmgr软件安装失败，请查看日志: /tmp/install_repmgr.log')
                exit()
        else:
            cmd = 'export PATH=' + self.dbhome + '/bin:$PATH && cd /home/postgres/soft/repmgr-' + version + \
                  ' && ./configure > /tmp/install_repmgr.log 2>>/tmp/install_repmgr.log' \
                  '&& make install 2>>/tmp/install_repmgr.log>> /tmp/install_repmgr.log 2>>' \
                  '/tmp/install_repmgr.log&& echo $?'
            value_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
            if "".join(value_cmd[-1]).strip('\n') == '0':
                print('Repmgr软件安装成功')
            else:
                print('Repmgr软件安装失败，请查看日志: /tmp/install_repmgr.log')
                exit()

        print('开始安装pglogical软件...')
        if self.os_user == 'root':
            cmd = 'su - postgres -c "cd /home/postgres/soft/pglogical-2.2.0 && make USE_PGXS=1' \
                  ' install > /tmp/install_pglogical.log" && echo $?'
            value_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
            if "".join(value_cmd[-1]).strip('\n') == '0':
                print('Pglogical软件安装成功')
            else:
                print('Pglogical软件安装失败，请查看日志: /tmp/install_pglogical.log')
                exit()
        else:
            cmd = 'export PATH=' + self.dbhome + '/bin:$PATH && cd /home/postgres/soft/pglogical-2.2.0 && make USE_PGXS=1' \
                  ' install > /tmp/install_pglogical.log 2>>/tmp/install_pglogical.log && echo $?'
        
            value_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
            if "".join(value_cmd[-1]).strip('\n') == '0':
                print('Pglogical软件安装成功')
            else:
                print('Pglogical软件安装失败，请查看日志: /tmp/install_pglogical.log')
                exit()

        print('开始安装pg_stat_statement插件')
        if self.os_user == 'root':
            cmd = 'su - postgres -c "cd /home/postgres/soft/postgresql-' + self.version + \
                  '/contrib/pg_stat_statements && make && make install" && echo $?'
            value_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
            if "".join(value_cmd[-1]).strip('\n') == '0':
                print('pg_stat_statement插件安装成功')
            else:
                print('pg_stat_statement插件安装失败')
                exit()
        else:
            cmd = 'cd /home/postgres/soft/postgresql-' + self.version + \
                  '/contrib/pg_stat_statements && make && make install && echo $?'
            value_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
            if "".join(value_cmd[-1]).strip('\n') == '0':
                print('pg_stat_statement插件安装成功')
            else:
                print('pg_stat_statement插件安装失败')
                exit()

    def init_db(self):
        print('开始初始化DB,请稍候...')
        self.check_dir(self.db_data)
        if chk == 0:
            cmd = 'ls ' + self.db_data + '|wc -l'
            value_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
            if "".join(value_cmd).strip('\n') == '0':
                pass
            else:
                print('provide directory ' + self.db_data + ' is not empty!')
                exit()
        else:
            if self.os_user == 'root':
                cmd = 'mkdir -p ' + self.db_data
                os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
                cmd = 'chown postgres:postgres ' + self.db_data
                os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
            else:
                print('提供的目录：' + self.db_data + '不存在')
                exit()

        archive_data = '/data1/archive'
        self.check_dir(archive_data)
        if chk == 0:
            cmd = 'ls ' + archive_data + '|wc -l'
            value_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
            if "".join(value_cmd).strip('\n') == '0':
                pass
            else:
                print('Arch directory ' + archive_data + ' is not empty!')
                exit()
        else:
            if self.os_user == 'root':
                cmd = 'mkdir -p ' + archive_data
                os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
                cmd = 'chown postgres:postgres ' + archive_data
                os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
            else:
                print('Arch 目录：' + archive_data + '不存在')
                exit()

        if self.os_user == 'root':
            cmd = 'su - postgres -c "echo \'postgres\' > /home/postgres/pwd.txt"'
            os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
            cmd = 'su - postgres -c "/usr/local/postgre' + self.version + '/bin/initdb -D ' + \
                  self.db_data +' -E UTF8 --locale=C -U postgres' \
                  ' --pwfile /home/postgres/pwd.txt 1>/tmp/init_db.log 2>/tmp/init_db.log" && echo $?'
            value_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
            if "".join(value_cmd[-1]).strip('\n') == '0':
                print('DB初始化成功')
            else:
                print('DB初始化失败，请查看日志: /tmp/init_db.log')
                exit()
        else:
            cmd = 'echo \'postgres\' > /home/' + self.dbuser + '/pwd.txt'
            os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
            cmd = self.dbhome + '/bin/initdb -D ' + \
                  self.db_data + ' -E UTF8 --locale=C -U postgres' \
                                 ' --pwfile /home/postgres/pwd.txt 1>/tmp/init_db.log 2>/tmp/init_db.log && echo $?'
            value_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
            if "".join(value_cmd[-1]).strip('\n') == '0':
                print('DB初始化成功')
            else:
                print('DB初始化失败，请查看日志: /tmp/init_db.log')
                exit()



        print('开始修改数据库初始化参数,请稍候...')

        cmd = 'scp -P ' + self.os_port + ' require_file/postgresql-' + self.version + '.conf ' + self.os_user + '@' \
              + self.ip + ':' + self.db_data + '/postgresql.conf && echo $?'
        self.trans_file(cmd)
        cmd = 'sed -i -e "s|port = 1923|port = ' + self.dbport + '|g" ' + self.db_data + '/postgresql.conf' 
        print(cmd)
        os_login.ssh(self.ip, self.os_user, self.os_port, cmd)

        print('开始启动数据库,请稍候...')
        if self.os_user == 'root':
            cmd = 'su - postgres -c "' + self.dbhome + '/bin/pg_ctl start -l logfile -D ' + self.db_data + '" && echo $?'
            value_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
            if "".join(value_cmd[-1]).strip('\n') == '0':
                print('DB启动成功')
            else:
                print('DB启动失败，请查看PG日志')
                exit()
        else:
            cmd = self.dbhome + '/bin/pg_ctl start -l logfile -D ' + self.db_data + ' && echo $?'
            value_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
            if "".join(value_cmd[-1]).strip('\n') == '0':
                print('DB启动成功')
            else:
                print('DB启动失败，请查看PG日志')
                exit()



class install_repmgr(install_pg):
    def __init__(self,osip,osuser,osport,dbuser,dbport,version,db_data):
        install_pg.__init__(self,osip,osuser,osport,dbuser,dbport,version,db_data)
        self.version = dict.get('repmgr_version')
        self.file_name = 'repmgr-' + self.version + '.tar.gz'
        self.primary_ip = "".join(dict.get('primary_host')).split(':').pop(1)
        self.standby_ip = "".join(dict.get('standby_host')).split(':').pop(1)
        self.primary_host = "".join(dict.get('primary_host')).split(':').pop(0)
        self.standby_host = "".join(dict.get('standby_host')).split(':').pop(0)
        self.sta_db_data = dict.get('standby_data')
        self.pri_db_data = dict.get('primary_data')


    def configure_file(self, ip, hostname, port_db, node_id, data_dir,rep_file):
        print('开始配置参数文件，请稍候...')
        cmd_dir = 'ls /home/postgres/repmgr'
        value_cmd_dir = os_login.ssh(ip, self.os_user, self.os_port, cmd_dir)
        if "".join(value_cmd_dir).strip('\n') != '':
            pass
        else:
            if self.os_user == 'root':
                cmd = 'su - postgres -c "mkdir repmgr"'
                os_login.ssh(ip, self.os_user, self.os_port, cmd)
            else:
                cmd = 'mkdir repmgr'
                os_login.ssh(ip, self.os_user, self.os_port, cmd)
        cmd_dir = 'ls ' + rep_file + '|wc -l'
        value_cmd_dir = os_login.ssh(ip, self.os_user, self.os_port, cmd_dir)
        if "".join(value_cmd_dir).strip('\n') == '1':
            print(rep_file + '已经存在，请修改配置文件！')
            exit()
        else:
            cp_rep_file = 'repmgr.conf'
            cmd = 'scp -P ' + self.os_port + ' require_file/repmgr.conf ' + self.os_user + '@' + \
                  ip + ':/home/postgres/repmgr/' + rep_file
            self.trans_file(cmd)

        cmd = 'sed -i -e \'s|node_id=|node_id=' + node_id + '|g\' /home/postgres/repmgr/' + rep_file
        os_login.ssh(ip, self.os_user, self.os_port, cmd)
        cmd = 'sed -i -e \'s|node_name=|node_name=' + hostname + '|g\' /home/postgres/repmgr/' + rep_file
        os_login.ssh(ip, self.os_user, self.os_port, cmd)
        cmd = 'sed -i -e "s|conninfo=|conninfo=\'host=' + hostname + \
              ' user=repmgr dbname=repmgr port=' + port_db + '\'|g" /home/postgres/repmgr/' + rep_file
        os_login.ssh(ip, self.os_user, self.os_port, cmd)
        cmd = 'sed -i -e \'s|data_directory=|data_directory=' + data_dir + '|g\' /home/postgres/repmgr/' + rep_file
        os_login.ssh(ip, self.os_user, self.os_port, cmd)
        cmd = 'sed -i -e \'s|pg_bindir=|pg_bindir=' + self.dbhome + '/bin|g\' /home/postgres/repmgr/' + rep_file
        os_login.ssh(ip, self.os_user, self.os_port, cmd)
        cmd = 'echo "promote_command=\'repmgr standby promote -f /home/postgres/repmgr/' + rep_file + \
              ' --log-to-file\'" >> /home/postgres/repmgr/' + rep_file
        os_login.ssh(ip, self.os_user, self.os_port, cmd)
        cmd = 'echo "follow_command=\'repmgr standby follow -f /home/postgres/repmgr/' + rep_file + \
              ' --log-to-file\'" >> /home/postgres/repmgr/' + rep_file
        os_login.ssh(ip, self.os_user, self.os_port, cmd)

        if self.os_user == 'root' and node_id == '1':
            cmd = 'su - postgres -c "repmgr -f /home/postgres/repmgr/' + rep_file + ' primary register"'
            os_login.ssh(ip, self.os_user, self.os_port, cmd)
        elif self.os_user != 'root' and node_id == '1':
            cmd = self.dbhome + '/bin/repmgr -f /home/postgres/repmgr/' + rep_file + ' primary register'
            os_login.ssh(ip, self.os_user, self.os_port, cmd)
        else:
            pass

    def install_rp(self):
	if dict.get('configure_pri') == 'y' or dict.get('configure_pri') == 'Y':
            cmd = 'scp -P ' + self.os_port + ' require_file/pg_hba.conf' + ' ' + self.os_user + '@' + \
                  self.ip + ':' +self.db_data + '/ && echo $?'
            self.trans_file(cmd)
            split = "".join(self.ip).rfind(".")
            cmd = 'echo "host    postgres        postgres      ' + self.ip[:split] + '.0/24          trust" >> '\
                  + self.db_data + '/pg_hba.conf'
            os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
            cmd = 'echo "host    replication        repmgr      ' + self.ip[:split] + '.0/24          trust" >> '\
                  + self.db_data + '/pg_hba.conf'
            os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
            cmd = 'echo "host    repmgr        repmgr      ' + self.ip[:split] + '.0/24          trust" >> '\
                  + self.db_data + '/pg_hba.conf'
            os_login.ssh(self.ip, self.os_user, self.os_port, cmd)

        if self.os_user == 'root':
            cmd = 'su - postgres -c "' + self.dbhome + '/bin/pg_ctl restart -D ' + self.db_data +' -l logfile"'
            os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
            cmd = 'su - postgres -c "' + self.dbhome + '/bin/createuser -h ' + self.ip + ' -p ' + \
                   self.dbport + ' -s repmgr && ' + self.dbhome + '/bin/createdb repmgr -h ' + self.ip + ' -p ' +self.dbport + ' -O repmgr"'
            os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
        else:
            cmd = self.dbhome + '/bin/pg_ctl restart -D ' + self.db_data + ' -l logfile'
            os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
            cmd = self.dbhome + '/bin/createuser -h ' + self.ip + ' -p ' +self.dbport + ' -s repmgr && ' + \
                  self.dbhome + '/bin/createdb repmgr -h ' + self.ip + ' -p ' +self.dbport + ' -O repmgr && echo $?'
            value_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
            if "".join(value_cmd).strip('\n') == '0':
		pass
            else:
		pass


    def clone_db(self,sta_rep_file):
        print('开始配置参数文件，请稍候...')
        if self.os_user == 'root':
            cmd = 'cat /etc/hosts |grep ' + self.primary_host + '|wc -l'
            value_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
            if "".join(value_cmd).strip('\n') == '0':
                cmd = 'echo "' + self.primary_ip + '    ' + self.primary_host + '" >> /etc/hosts'
                os_login.ssh(self.primary_ip, self.os_user, self.os_port, cmd)
                cmd = 'echo "' + self.standby_ip + '    ' + self.standby_host + '" >> /etc/hosts'
                os_login.ssh(self.primary_ip, self.os_user, self.os_port, cmd)
                if self.primary_ip == self.standby_ip:
                    pass
                else:
                    cmd = 'echo "' + self.primary_ip + '    ' + self.primary_host + '" >> /etc/hosts'
                    os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)
                    cmd = 'echo "' + self.standby_ip + '    ' + self.standby_host + '" >> /etc/hosts'
                    os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)
            else:
                pass
        else:
            cmd = 'cat /etc/hosts |grep ' + self.primary_host + '|wc -l'
            value_cmd = os_login.ssh(self.ip, self.os_user, self.os_port, cmd)
            if "".join(value_cmd).strip('\n') == '0':
                print('请写入主机信息至/etc/hosts中')
		exit()
            else:
                pass


        print('开始配置备库')
        print(self.sta_db_data)
        self.configure_file(self.standby_ip,self.standby_host,self.dbport,dict.get('sta_node_id'),
                            self.sta_db_data,rep_file)

        if self.os_user == 'root':
            print('开始克隆，请稍候...')
            cmd = 'su - postgres -c "' + self.dbhome + '/bin/repmgr -h ' + self.primary_ip + \
                  ' -U repmgr -d repmgr -p ' + self.dbport + ' -f /home/postgres/repmgr/' + sta_rep_file + \
                  ' standby clone > /tmp/Clone_DB.log 2>>/tmp/Clone_DB.log" && echo $?'
        else:
            print('开始克隆，请稍候...')
            cmd = self.dbhome + '/bin/repmgr -h ' + self.primary_ip + \
                  ' -U repmgr -d repmgr -p ' + self.dbport + ' -f /home/postgres/repmgr/' + sta_rep_file + \
                  ' standby clone > /tmp/Clone_DB.log 2>>/tmp/Clone_DB.log && echo $?'
        value_cmd = os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)
        if "".join(value_cmd[-1]).strip('\n') == '0':
            print('DB克隆成功')
        elif value_cmd is None:
            print('DB克隆失败，请查看日志: /tmp/Clone_DB.log')
            exit()
	else:
	    print('DB克隆失败，请查看日志: /tmp/Clone_DB.log')
            exit()

        if self.os_user == 'root':
            cmd = 'su - postgres -c "' + self.dbhome + '/bin/pg_ctl start -l logfile -D ' + self.sta_db_data + '"'
            os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)
            cmd = 'su - postgres -c "repmgr -f /home/postgres/repmgr/' + sta_rep_file + ' standby register"'
            os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)
        else:
            cmd = self.dbhome + '/bin/pg_ctl start -l logfile -D ' + self.sta_db_data
            print(cmd)
            os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)
            cmd = self.dbhome + '/bin/repmgr -f /home/postgres/repmgr/' + sta_rep_file + ' standby register'
            os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)



    def switchover(self,sta_rep_file):
        print('开始执行switchover,请稍候...')
        cmd = 'ls /home/postgres/repmgr/' + sta_rep_file + '|wc -l'
        value_cmd = os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)
        if "".join(value_cmd).strip('\n') == '0':
            print(self.repmgr + '不存在，请确认！')
            exit()
        else:
            pass

	if self.os_user == 'root':
            cmd = 'su - postgres -c "' + self.dbhome + '/bin/repmgr standby switchover -f /home/postgres/repmgr/' + \
                  sta_rep_file + '  --siblings-follow > /tmp/switch_DB.log" && echo $?'
            value_cmd = os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)
            if "".join(value_cmd[-1]).strip('\n') == '0':
                print('DB切换成功')
            else:
                print('DB切换失败，请查看日志: /tmp/switch_DB.log')
                exit()
	else:
            cmd = self.dbhome + '/bin/repmgr standby switchover -f /home/postgres/repmgr/' + sta_rep_file + \
                                                         ' > /tmp/switch_DB.log 2>>/tmp/switch_DB.log && echo $?'
	    print(cmd)
            value_cmd = os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)
            if "".join(value_cmd[-1]).strip('\n') == '0':
                print('DB切换成功')
            else:
                print('DB切换失败，请查看日志: /tmp/switch_DB.log')
                exit()

    def failover(self,sta_rep_file):
        print('开始执行Failover,请稍候...')
        if self.os_user == 'root':
            cmd = 'su - postgres -c "' + self.dbhome + '/bin/repmgr -f /home/postgres/repmgr/' + sta_rep_file + \
                  ' standby unregister"'
            os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)
            cmd = 'su - postgres -c "' + self.dbhome + '/bin/pg_ctl stop -D ' + self.sta_db_data + ' -m fast " && echo $?'
            value_cmd = os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)
            if "".join(value_cmd[-1]).strip('\n') == '0':
                print('DB关停成功')
            else:
                print('DB关停失败，请查看PG日志')
                exit()
            cmd = 'mv ' + self.sta_db_data + '/recovery.conf ' + self.dbhome + '/recovery.conf.bak'
            os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)
            cmd = 'su - postgres -c "' + self.dbhome + '/bin/pg_ctl -l logfile start -D ' + self.sta_db_data + '" && echo $?'
            value_cmd = os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)
            if "".join(value_cmd[-1]).strip('\n') == '0':
                print('DB启动成功')
            else:
                print('DB启动失败，请查看PG日志')
                exit()

	    time.sleep(30)
            sql = 'su - postgres -c "' + self.dbhome + '/bin/psql -t -A -h ' + self.standby_ip + ' -p ' + self.dbport + \
                  ' -U repmgr -d repmgr -c "delete from repmgr.nodes"\"'
            os_login.ssh(self.standby_ip, self.os_user, self.os_port, sql)
            cmd = 'su - postgres -c "' + self.dbhome + '/bin/repmgr -f /home/postgres/repmgr/' + sta_rep_file + \
                  ' primary register > /tmp/rep_regis.txt" && echo $?'
            value_cmd = os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)
            if "".join(value_cmd[-1]).strip('\n') == '0':
                print('DB切换主库完成！')
            else:
                print('DB注册失败，请查看PG日志 : /tmp/rep_regis.txt')
                exit()
	else:
	    cmd = self.dbhome + '/bin/repmgr -f /home/postgres/repmgr/' + sta_rep_file + \
                  ' standby unregister'
            os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)
            cmd = self.dbhome + '/bin/pg_ctl stop -D ' + self.sta_db_data + ' -m fast  && echo $?'
            value_cmd = os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)
            if "".join(value_cmd[-1]).strip('\n') == '0':
                print('DB关停成功')
            else:
                print('DB关停失败，请查看PG日志')
                exit()
            cmd = 'mv ' + self.sta_db_data + '/recovery.conf ' + self.dbhome + '/recovery.conf.bak'
            os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)
            cmd = self.dbhome + '/bin/pg_ctl start -l logfile -D ' + self.sta_db_data + ' && echo $?'
            value_cmd = os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)
            if "".join(value_cmd[-1]).strip('\n') == '0':
                print('DB启动成功')
            else:
                print('DB启动失败，请查看PG日志')
                exit()

            sql = self.dbhome + '/bin/psql -t -A -h ' + self.standby_ip + ' -p ' + self.dbport + \
                  ' -U repmgr -d repmgr -c "delete from repmgr.nodes" > /tmp/exec_sql.txt 2>>/tmp/exec_sql.txt'
	    print(sql)
            os_login.ssh(self.standby_ip, self.os_user, self.os_port, sql)
            cmd = self.dbhome + '/bin/repmgr -f /home/postgres/repmgr/' + sta_rep_file + \
                  ' primary register > /tmp/rep_regis.txt && echo $?'
 	    print(cmd)
            value_cmd = os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)
            if "".join(value_cmd[-1]).strip('\n') == '0':
                print('DB切换主库完成！')
            else:
                print('DB注册失败，请查看PG日志 : /tmp/rep_regis.txt')
                exit()

    def rep_rejoin(self,sta_rep_file):
        print('开始执行Rejoin过程,请稍候...')
        if self.os_user == 'root':
            cmd = 'su - postgres -c "' + self.dbhome + '/bin/repmgr -f /home/postgres/repmgr/' + sta_rep_file + \
                  ' node rejoin -d \'host=' + self.primary_ip + \
                  ' dbname=repmgr user=repmgr\' --force-rewind --verbose > /tmp/rejoin_db.txt" && echo $?'
            value_cmd = os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)
            if "".join(value_cmd[-1]).strip('\n') == '0':
                print('DB Rejoin集群完成！')
            else:
                print('DB Rejoin集群失败，请查看PG日志 : /tmp/rejoin_db.txt')
                exit()
            cmd = 'su - postgres -c "' + self.dbhome + '/bin/repmgr -f /home/postgres/repmgr/' + sta_rep_file + \
                  ' standby register "'
            os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)
	else:
	    cmd = self.dbhome + '/bin/pg_ctl stop -D ' + self.sta_db_data
            value_cmd = os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)
	    cmd = 'mv ' + self.sta_db_data + '/recovery.conf.bak ' + self.sta_db_data + '/recovery.conf'
            os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)
            cmd = 'sed -i \'/primary_conninfo/d\' ' + self.sta_db_data + '/recovery.conf'
            os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)
            cmd = 'echo "primary_conninfo = \'host=' + self.standby_host + ' user=repmgr port=' + self.dbport + ' application_name=' +self.standby_host + '\"'
            os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)
            cmd = 'sed -i \'/primary_slot_name/d\' ' + self.sta_db_data + '/recovery.conf'
            os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd) 
	    cmd = 'rsync -cva -P --inplace ' + self.pri_db_data + ' ' + self.standby_ip +':' + self.sta_db_data + ' > /tmp/rejoin.txt 2>>/tmp/rejoin.txt && echo $?'
	    value_cmd = os_login.ssh(self.primary_ip, self.os_user, self.os_port, cmd)
            if "".join(value_cmd[-1]).strip('\n') == '0':
                print('数据同步完成！')
            else:
                print('数据同步失败,请尝试删除备库数据重新克隆...')
                exit()
            cmd = self.dbhome + '/bin/pg_ctl start -l logfile -D ' + self.sta_db_data + ' && echo $?'
            value_cmd = os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)
            if "".join(value_cmd[-1]).strip('\n') == '0':
		print('DB启动成功!!')
                print('DB Rejoin集群完成！')
            else:
                print('DB Rejoin集群失败,请尝试删除备库数据重新克隆...')
                exit()
            cmd = self.dbhome + '/bin/repmgr -f /home/postgres/repmgr/' + sta_rep_file + \
                  ' standby register -F'
            os_login.ssh(self.standby_ip, self.os_user, self.os_port, cmd)


def get_params():
    f=open('config','r')
    try:
        for line in f:
            if (line.startswith(';')) or (line == '\n') or (line.startswith('  ')):
                pass
            else:
                get_param = line.split('#').pop(0).strip('\n').split('=').pop(0).strip()
                get_value = line.split('#').pop(0).strip('\n').split('=').pop(1).strip()
                dict[get_param] = get_value
    finally:
        f.close()

if __name__=="__main__":
    get_params()
    osip = dict.get('ip')
    osuser = dict.get('os_user')
    osport = dict.get('os_port')
    db_user = dict.get('db_user')
    dbport = dict.get('db_port')
    db_version = dict.get('db_version')
    db_data = dict.get('db_data')
    pri_node_id = dict.get('pri_node_id')
    repmgr_version = dict.get('repmgr_version')
    step = dict.get('opera_type').replace(',','')
    hostname = dict.get('hostname')
    rep_file = dict.get('sta_repmgr_file')
    pg_file = 'postgresql-' + db_version + '.tar.gz'
    repmgr_file = 'repmgr-' + repmgr_version + '.tar.gz'
    install = install_pg(osip,osuser,osport,db_user,dbport,db_version,db_data)
    repmgr = install_repmgr(osip,osuser,osport,db_user,dbport,db_version,db_data)

    for i in step:
        if   i == '1':
            install.prepare()
            install.put_file(pg_file)
            install.install()
            repmgr.put_file(repmgr_file)
            install.install_soft(repmgr_version)
        elif i == '2':
            install.init_db()
        elif i == '3':
            repmgr.put_file(repmgr_file)
            install.install_soft(repmgr_version)
            repmgr.install_rp()
            if dict.get('configure_pri') == 'y' or dict.get('configure_pri') == 'Y':
                repmgr.configure_file(osip,hostname,dbport,pri_node_id,db_data,rep_file)
        elif i == '4':
            repmgr.clone_db(rep_file)
        elif i == '5':
            repmgr.switchover(rep_file)
        elif i == '6':
            repmgr.failover(rep_file)
        elif i == '7':
            repmgr.rep_rejoin(rep_file)
        else:
            pass




