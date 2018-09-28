'''
    lib v0.1
    2018-09-07
'''
import paramiko
import configparser
from kazoo.client import KazooClient

class ServiceExistException(Exception):
    """
    when service name exist, raise this exception
    """
    def __init__(self):
        self.msg = 'service name already exists.'

class SSH_demo():
    """
    A class includes methods about ssh connection
    """

    def __init__(self):
        """
        create a ssh session
        """
        self.ssh = paramiko.SSHClient()
        self.key = paramiko.AutoAddPolicy()
        self.ssh.set_missing_host_key_policy(self.key)

    def connect_to_host(self,host):
        """
        ssh connect via host ip
        :param host: ip
        :return: ssh session
        """
        try:
            self.ssh.connect(host,22,'root','nihao',timeout=5)
            return self.ssh
        except:
            print('Please check your ip')
            exit()

    def connect_to_host_via_hostname(self,hostname):
        """
        ssh connection via host name
        method gets hostname by analyzing config.ini
        :param hostname: hostname
        :return: ssh session
        """
        try:
            conf = configparser.ConfigParser()
            conf.read('config.ini')
            ip = conf.get('hostmap',hostname)
            self.ssh.connect(ip,22,'root','nihao',timeout=5)
            return self.ssh
        except:
            # 添加日志
            print('Please check your hostname.')

    def close_ssh_session(self):
        """
        close ssh session
        :return:None
        """
        self.ssh.close()

class Sftp_demo():
    """
    transport file via ssh connection
    """
    def __init__(self, host):
        """
        create a sftp session
        """
        self.transport = paramiko.Transport((host, 22))
        self.transport.connect(username='root', password='nihao')

    def put(self, filename, path):
        """
        push file to remote path
        :param filename:
        :param path:
        :return: None
        """
        sftp = paramiko.SFTPClient.from_transport(self.transport)
        sftp.put(filename, path)

    def close(self):
        """
        close the connection
        :return:
        """
        self.transport.close()


class Zookeeper_Demo():
    """
    A class includes zookeeper operations
    """
    def __init__(self):
        """
        create a zookeeper session by polling host from config.ini
        """
        conf = configparser.ConfigParser()
        conf.read('config.ini')
        zk_ip = conf.get('zookeeper','host')
        cluster_ip = zk_ip.split(',')

        conn_flag = 1
        for item in cluster_ip:
            try:
                conn_flag = 1
                self.zk = KazooClient(hosts=item,read_only=False,timeout=0.5)
                self.zk.start(timeout=0.5)
            except:
                print(item + " is unreachable")
                conn_flag = 0
            if conn_flag == 1:
                break

        if conn_flag == 0:
            print("There is no available node in zookeeper cluster")
            raise IOError

    def check_service_name(self, service):
        """
        check whether the service name already exist
        :param service: service name
        :return: boolean
        """
        exists = self.zk.get_children('/rds_demo/service')
        try:
            if service in exists:
                raise ServiceExistException
        except ServiceExistException as e:
            print(e.msg)
            self.zk.stop()
            exit()

    def create_standalone(self, service, container, host, port):
        """
        when create a standalone postgres service, write msg to zookeeper
        :param service: service name
        :param container: container name
        :param host:'xxx.xxx.xx.xx'
        :param port:'xxxx'
        :return:None
        """
        # get hostname @hostname
        try:
            conf = configparser.ConfigParser()
            conf.read('config.ini')
            ip_list = map(lambda x : conf.get('hostmap',x),conf.options('hostmap'))
            index = list(ip_list).index(host)
            hostname = conf.options('hostmap')[index]
        except:
            print('There is no %s in cluster.' %(host))
            raise ValueError
        # append msg of container name @container
        container_msg = self.zk.get('/rds_demo/%s/containers' %(hostname))
        containers_str = container_msg[0].decode('utf-8')
        containers_list = containers_str.split(',')
        containers_list.append(container)
        if '' in containers_list:
            containers_list.remove('')
        containers_str = ','.join(containers_list)
        container_msg = bytes(containers_str, encoding='utf-8')
        self.zk.set('/rds_demo/%s/containers' %(hostname), container_msg)
        # zookeeper service information
        self.zk.create('/rds_demo/service/%s' %(service))
        self.zk.create('/rds_demo/service/%s/isMS' %(service), b'False')
        self.zk.create('/rds_demo/service/%s/host' %(service),
                       bytes(hostname, encoding='utf-8'))
        self.zk.create('/rds_demo/service/%s/port' %(service),
                       bytes(port, encoding='utf-8'))
        self.zk.create('/rds_demo/service/%s/container' %(service),
                       bytes(container, encoding='utf-8'))

    def create_ms(self, service, master, master_port, slave, slave_port):
        """
        create a master-slave service
        the container name will be given by the following rules
        master container : ${service_name}_m
        slave container : ${service_name}_s
        :param service: service name
        :param master: 'xxx.xxx.xxx.xxx'
        :param master_port: 'xxxx'
        :param slave: 'xxx.xxx.xxx.xxx'
        :param slave_port: 'xxxx'
        :return: None
        """
        def get_host_name(host_ip):
            try:
                conf = configparser.ConfigParser()
                conf.read('config.ini')
                ip_list = map(lambda x: conf.get('hostmap', x), conf.options('hostmap'))
                index = list(ip_list).index(host_ip)
                hostname = conf.options('hostmap')[index]
            except:
                print('There is no %s in cluster.' % (host_ip))
                raise ValueError
            return hostname
        # get master's host name
        master_host = get_host_name(master)
        # get slave's host name
        slave_host = get_host_name(slave)
        def append_container_list(hostname, container):
            container_msg = self.zk.get('rds_demo/%s/containers' %(hostname))
            container_str = container_msg[0].decode('utf-8')
            container_list = container_str.split(',')
            container_list.append(container)
            if '' in container_list:
                container_list.remove('')
            container_str = ','.join(container_list)
            container_msg = bytes(container_str, encoding='utf-8')
            self.zk.set('rds_demo/%s/containers' %(hostname), container_msg)
        # master container name
        master_container_name = service +'_m'
        # slave container name
        slave_container_name = service + '_s'
        # append container name
        append_container_list(master_host, master_container_name)
        append_container_list(slave_host, slave_container_name)
        # zookeeper service information
        self.zk.create('/rds_demo/service/%s' %(service))
        self.zk.create('/rds_demo/service/%s/isMS' %(service), b'True')
        self.zk.create('/rds_demo/service/%s/master' %(service))
        self.zk.create('/rds_demo/service/%s/slave' %(service))
        self.zk.create('/rds_demo/service/%s/master/host' %(service),
                       bytes(master_host, encoding='utf-8'))
        self.zk.create('/rds_demo/service/%s/master/port' %(service),
                       bytes(master_port, encoding='utf-8'))
        self.zk.create('rds_demo/service/%s/master/container' %(service),
                       bytes(master_container_name, encoding='utf-8'))
        self.zk.create('/rds_demo/service/%s/slave/host' % (service),
                       bytes(slave_host, encoding='utf-8'))
        self.zk.create('/rds_demo/service/%s/slave/port' % (service),
                       bytes(slave_port, encoding='utf-8'))
        self.zk.create('rds_demo/service/%s/slave/container' % (service),
                       bytes(slave_container_name, encoding='utf-8'))

    def check_service_type(self, service):
        """
        check the type of the service via service name
        :param service: the
        :return: string
        """
        isMS = self.zk.get('/rds_demo/service/%s/isMS' %(service))
        isMS = isMS[0].decode('utf-8')
        if isMS == 'False':
            return 'standalone'
        elif isMS == 'True':
            return 'm-s'
        else:
            print('error imformation in zookeeper.')
            raise ValueError

    def get_host_via_service(self, service):
        """
        get host via service name
        :param service: service name
        :param isMS: service type
        :return: hostname
        """
        isMS = (self.zk.get('/rds_demo/service/%s/isMS'
                            %(service)))[0].decode('utf-8')
        if isMS == 'False':
            hostname = self.zk.get('/rds_demo/service/%s/host' %(service))
            hostname = hostname[0].decode('utf-8')
        elif isMS == 'True':
            master_host = self.zk.get('/rds_demo/service/%s/master/host'
                                      %(service))[0].decode('utf-8')
            slave_host = self.zk.get('/rds_demo/service/%s/slave/host'
                                     %(service))[0].decode('utf-8')
            hostname = [master_host, slave_host]
        else:
            print('error information in zookeeper.')
            raise ValueError

        return hostname

    def get_container_via_service(self, service):
        """
        get container name via service name
        :param service:
        :param isMS:
        :return:
        """
        isMS = (self.zk.get('/rds_demo/service/%s/isMS'
                            %(service)))[0].decode('utf-8')
        if isMS == 'False':
            container = self.zk.get('/rds_demo/service/%s/container' % (service))
            container = container[0].decode('utf-8')
        elif isMS == 'True':
            master_container = self.zk.get('/rds_demo/service/%s/master/container'
                                           % (service))[0].decode('utf-8')
            slave_container = self.zk.get('/rds_demo/service/%s/slave/container'
                                          % (service))[0].decode('utf-8')
            container = [master_container, slave_container]
        else:
            print('error information in zookeeper.')
            raise ValueError

        return container

    def delete_standalone_message(self, service):
        """
        when deleting a standalone service, remove the zookeeper msg
        :param service: name of the service
        :return: None
        """
        hostname = self.zk.get('/rds_demo/service/%s/host' %(service))
        hostname = hostname[0].decode('utf-8')
        container = self.zk.get('/rds_demo/service/%s/container' %(service))
        container = container[0].decode('utf-8')
        con_msg = self.zk.get('/rds_demo/%s/containers' %(hostname))
        con_str = con_msg[0].decode('utf-8')
        con_list = con_str.split(',')
        con_list.remove(container)
        con_str = ','.join(con_list)
        self.zk.set('/rds_demo/%s/containers' %(hostname),
                    bytes(con_str, encoding='utf-8'))
        # delete the service record
        self.zk.delete('/rds_demo/service/%s' %(service), recursive=True)

    def delete_ms_message(self, service):
        """
        when deleting a master-slave service, remove the zookeeper msg
        :param service: service name
        :return: None
        """
        master_host = self.zk.get('/rds_demo/service/%s/master/host'
                                  %(service))[0].decode('utf-8')
        master_container = self.zk.get('/rds_demo/service/%s/master/container'
                                       %(service))[0].decode('utf-8')
        slave_host = self.zk.get('/rds_demo/service/%s/slave/host'
                                 % (service))[0].decode('utf-8')
        slave_container = self.zk.get('/rds_demo/service/%s/slave/container'
                                      % (service))[0].decode('utf-8')
        def remove_container_list(hostname, container):
            container_msg = self.zk.get('/rds_demo/%s/containers' %(hostname))
            container_str = container_msg[0].decode('utf-8')
            container_list = container_str.split(',')
            container_list.remove(container)
            container_str = ','.join(container_list)
            container_msg = bytes(container_str, encoding='utf-8')
            self.zk.set('/rds_demo/%s/containers' %(hostname), container_msg)
        # delete master container record
        remove_container_list(master_host, master_container)
        # delete slave container record
        remove_container_list(slave_host, slave_container)
        # delete service record
        self.zk.delete('/rds_demo/service/%s' %(service), recursive=True)

    def close(self):
        """
        close a zookeeper session
        :return:None
        """
        self.zk.stop()