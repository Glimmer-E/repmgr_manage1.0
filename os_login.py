# -*- coding: UTF-8 -*-
import paramiko
def ssh(ip,user,port,cmd):
    try:
        # private_key_path = '/home/postgres/.ssh/id_rsa'
        # key = paramiko.RSAKey.from_private_key_file(private_key_path)
        os=paramiko.SSHClient()
        os.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        os.connect(hostname=ip,port=port,username=user,password='melot2o15')
        stdin, stdout, stderr=os.exec_command(cmd)
        results = stdout.readlines()
        return results
        os.close()
    except Exception as ex:
        print ("\tError %s\n" % ex)
        exit()

def trans(ip,user,port,file_name,r_dir):
    try:
        transport = paramiko.Transport((ip,port))
        transport.connect(username=user,password='postgres')
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.put(file_name,r_dir)
        transport.close()
    except Exception, e:
        print('上传文件失败！')

