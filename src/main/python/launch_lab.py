import subprocess
import os
import sys

inp_dir = "/lab2"
res_dir = "/lab2_res"

launch_count = 0


def rm_res_dir():
    subprocess.call(["/usr/bin/hdfs", "dfs", "-rm", "-r", res_dir])


def launch_with_keys(keys):
    global launch_count
    rm_res_dir()
    os.environ["HADOOP_NAMENODE_OPTS"] = keys
    out_file = open('./result'+str(launch_count), 'w')
    subprocess.call(["/usr/bin/hadoop", "jar", "./../../../build/libs/lab2.jar", "epam.Main", inp_dir, res_dir],
                    stdout=out_file)
    launch_count += 1


if __name__ == '__main__':
    launch_with_keys("")
    launch_with_keys("--Xms8g")
    launch_with_keys("--Xmx8g --Xmn2g -XX:PermSize=64M -XX:MaxPermSize=256M")
    launch_with_keys("-XX:ParallelGCThreads=8")
    launch_with_keys("-XX:+UseCMSInitiatingOccupancyOnly")
    launch_with_keys("-XX:CMSInitiatingOccupancyFraction=70")
