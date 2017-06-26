from fabric.api import *

env.key_filename = '~/server'
env.roledefs = {
        'server': {'hosts': ['tangh@server']},
        'client': {'hosts': ['tangh@server', 'hult@localhost']}
        }

@task
@parallel
@roles('client')
def sync():
    put('./target/dummy-client-fat.jar', '~/dummy-client-fat.jar')
    put('./config.json', '~/config.json')

@task
@roles('server')
def start():
    run('~/init', pty=False)


@task
@parallel
@roles('client')
def init():
    run('java -jar ~/dummy-client-fat.jar 2> ~/client.log 1> client.out&', pty=False)

@task
@parallel
@roles('client')
def input():
    run("echo '10' | nc -u -q1 127.0.0.1 9999", pty=False)

def version():
    run('java -version')

@task
@roles('client')
def check():
    run('cat ~/client.out')

@task
@roles('client')
def result():
    run('cat ~/client.out')

@task
@roles('client')
def jps():
    run('jps')

@task
@parallel
@roles('client')
def gather():
    run("grep 'rtt:' ~/client.log |awk '{print $6}' > rtt.txt")
    get('~/client.out', './%(host)s_client.out')
    get('~/rtt.txt', './%(host)s_rtt.log')

