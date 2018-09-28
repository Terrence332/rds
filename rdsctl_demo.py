import sys
import argparse
import configparser
import time
import os
from lib import SSH_demo
from lib import Zookeeper_Demo
from lib import Sftp_demo

def create_standalone_container(name, host, port):
    """
    create a standalone
    :return:None
    """
    # create a Zookeeper_Demo
    zk = Zookeeper_Demo()
    # start a ssh session
    ssh_session = SSH_demo()
    ssh = ssh_session.connect_to_host(host)
    # check service name
    service_name = name
    zk.check_service_name(service_name)
    # container name equals to service name in standalone model
    container_name = name
    # get volume map path
    conf = configparser.ConfigParser()
    conf.read('config.ini')
    base_path = conf.get('dir', 'datadir')
    volume = base_path + '/' + service_name + '/data'
    ssh.exec_command('mkdir -p %s' %(volume))
    ssh.exec_command('docker run -d \
        --name %s \
        -p %s:5432 \
        -e POSTGRES_PASSWORD=nihao \
        -v %s:/var/lib/postgresql/data \
        postgres' %(container_name, port, volume))
    # write message to zookeeper
    zk.create_standalone(service_name, container_name, host, port)
    # close ssh session
    ssh_session.close_ssh_session()
    # close zookeeper connection
    zk.close()

def create_ms_containers(name, master, master_port, slave, slave_port):
    """
    create the master node of postgres
    :return:None
    """
    # create a Zookeeper_Demo
    zk = Zookeeper_Demo()
    # check service name
    zk.check_service_name(name)
    # create master ssh session
    ssh_session_master = SSH_demo()
    ssh_master = ssh_session_master.connect_to_host(master)
    # create slave ssh session
    ssh_session_slave = SSH_demo()
    ssh_slave = ssh_session_slave.connect_to_host(slave)
    # get volume map path
    conf = configparser.ConfigParser()
    conf.read('config.ini')
    base_path = conf.get('dir', 'datadir')
    volume_master = base_path + '/' + name + '/master'
    volume_slave = base_path + '/' + name + '/slave'
    try:
        ssh_master.exec_command('mkdir -p %s' % (volume_master))
        ssh_slave.exec_command('mkdir -p %s' % (volume_slave))
    except:
        print('Error occurred during mkdir')
    # run container
    master_container_name = name + '_m'
    slave_container_name = name + '_s'
    def docker_command(container_name, port, volume):
        str = 'docker run -d \
            --name %s \
            -p %s:5432 \
            -e POSTGRES_PASSWORD=nihao \
            -v %s:/var/lib/postgresql/data \
            postgres' %(container_name, port, volume)
        return str
    ssh_master.exec_command(docker_command(master_container_name, master_port, volume_master))
    ssh_slave.exec_command(docker_command(slave_container_name, slave_port, volume_slave))
    # get conf file path
    conf = configparser.ConfigParser()
    conf.read('config.ini')
    pg_hba = conf.get('filepath', 'pg_hba')
    master_conf = conf.get('filepath', 'master_conf')
    slave_conf = conf.get('filepath', 'slave_conf')
    time.sleep(1)
    ssh_master.exec_command('cp -f %s %s/pg_hba.conf' %(pg_hba, volume_master))
    ssh_master.exec_command('cp -f %s %s/postgresql.conf' %(master_conf, volume_master))
    ssh_slave.exec_command('cp -f %s %s/postgresql.conf' %(slave_conf, volume_slave))
    # make recovery.conf
    def make_recovery_conf(host, port):
        f = open('recovery.conf', 'a+')
        f.write('standby_mode = \'on\'' + '\n')
        str = ('primary_conninfo = \'host=%s port=%s user=replica password=nihao\''
               %(host, port))
        f.write(str)
    make_recovery_conf(master, master_port)
    # create sftp connection
    sftp = Sftp_demo(slave)
    sftp.put('recovery.conf', '%s/recovery.conf' %(volume_slave))
    ssh_master.exec_command('docker exec %s psql -U postgres -c \"create role replica login replication encrypted password \'nihao\';\"'
                            %(master_container_name))
    # stop container
    ssh_master.exec_command('docker stop %s' %(master_container_name))
    ssh_slave.exec_command('docker stop %s' %(slave_container_name))
    time.sleep(1)
    ssh_master.exec_command('rsync -cva --inplace --exclude=*pg_xlog* %s/ %s:%s/'
                            %(volume_master, slave, volume_slave))
    print('rsync -cva --inplace --exclude=*pg_xlog* %s/ %s:%s/'
                            %(volume_master, slave, volume_slave))
    time.sleep(1)
    # start container
    ssh_master.exec_command('docker start %s' %(master_container_name))
    ssh_slave.exec_command('docker start %s' %(slave_container_name))
    # delete recovery file
    os.remove('recovery.conf')
    # close sftp session
    sftp.close()
    # close ssh session
    ssh_session_master.close_ssh_session()
    ssh_session_slave.close_ssh_session()
    # close zookeeper connection
    zk.close()

def stop_service(name):
    """
    stop the container of a service via service name
    :param name: service name
    :return:None
    """
    pass

def delete_service(service):
    """
    delete a postgres service
    :param service: service name
    :return:None
    """
    # create a Zookeeper_Demo
    zk = Zookeeper_Demo()
    type = zk.check_service_type(service)
    # if the type is standalone
    if type == 'standalone':
        # start a ssh session
        ssh_session = SSH_demo()
        hostname = zk.get_host_via_service(service)
        container = zk.get_container_via_service(service)
        ssh = ssh_session.connect_to_host_via_hostname(hostname)
        ssh.exec_command('docker rm -f %s' %(container))
        # delete zookeeper msg
        zk.delete_standalone_message(service)
        # get volume map path
        conf = configparser.ConfigParser()
        conf.read('config.ini')
        base_path = conf.get('dir', 'datadir')
        volume = base_path + '/' + service
        # delete data path
        ssh.exec_command('rm -rf %s' %(volume))
        # close ssh session
        ssh_session.close_ssh_session()
    # if the type is master-slave
    elif type == 'm-s':
        pass
    else:
        print('Error information in zookeeper.')
        exit()
    # close zookeeper connection
    zk.close()

def restart_service(name):
    """
    restart a postgres service
    :return:None
    """
    pass


def print_help_message():
    """
    print help messages of the first command level
    :return:None
    """
    print('''Commands:
  create    create a new master-slave or standalone postgres
  delete    delete a service
  restart   restart a service
  ---------------rds_server v0.1---------------
  warning: only ports above 20000 can be used.''')

if __name__ == '__main__':
    # command system
    parser = argparse.ArgumentParser()
    # positional arguments
    parser.add_argument('option')

    if len(sys.argv) == 1:
        print_help_message()

    elif sys.argv[1] == '-h' or sys.argv[1] == '--help':
        print_help_message()

    elif sys.argv[1] == 'create':

        if len(sys.argv) == 2:
            print('Run rdsctl COMMAND --help for more information on a command')
        else:
            parser.add_argument('-n','--name',
                                required=True,
                                help='name of the postgres service')
            parser.add_argument('-t','--type',
                                choices=['m-s','standalone'],
                                default='standalone',
                                help='master-slave or standalone mode')
            parser.add_argument('-m','--master',
                                default='192.168.7.13',
                                help='ip or hostname of master node')
            parser.add_argument('-p','--port',
                                default='100',
                                help='port of the master node')
            parser.add_argument('-s','--slave',
                                default='192.168.7.13',
                                help='ip or hostname of slave node')
            parser.add_argument('-sp','--slave_port',
                                default='100',
                                help='port of the slave node')
            args = parser.parse_args()
            if args.type == 'standalone':
                # check the port
                if int(args.port) <=20000:
                    print('the port should be above 20000')
                    raise ValueError
                else:
                    create_standalone_container(args.name, args.master, args.port)
            elif args.type == 'm-s':
                # check the port
                if int(args.port) <= 20000 or int(args.slave_port) <= 20000:
                    print('the port should be above 20000')
                    raise ValueError
                elif args.master == args.slave:
                    print('please choose different ip for master and slave')
                    raise ValueError
                else:
                    create_ms_containers(args.name, args.master, args.port,
                                         args.slave, args.slave_port)

    elif sys.argv[1] == 'stop':
        parser.add_argument('-n', '--name',
                            required=True,
                            help='name of the postgres service')
        args = parser.parse_args()
        stop_service(args.name)

    elif sys.argv[1] == 'delete':
        parser.add_argument('-n', '--name',
                            required=True,
                            help='name of the postgres service')
        args = parser.parse_args()
        delete_service(args.name)


    elif sys.argv[1] == 'restart':
        parser.add_argument('-n', '--name',
                            required=True,
                            help='name of the postgres service')
        args = parser.parse_args()
        restart_service(args.name)
