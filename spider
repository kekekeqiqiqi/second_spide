# my_first_spide

# 实验室第一个任务

# 完成时间  2019/9/20
#对数据的处理分析

# import  csv
import time

import numpy as np
import  pandas as pd
from pandas import read_csv
from pandas import read_hdf


class modo:

    def open_file(self,address):

        h_5=pd.HDFStore(address,mode='r')
        csv_frame=h_5.get('data')
        h_5.close()

        return csv_frame

    def cal_day(self,note):

        note =note.sort_values(ascending=False)
        modo = note.reset_index(drop=True)
        mo=modo[0]
        add=0
        sum=0


        for i in range(0,len (modo)):

            if sum>=len(modo)-i:
                break
            if modo[i] != mo :
                if add>sum:
                    sum=add
                add=1
                mo=modo[i]
            else:
                add+=1
                if i ==len(modo)-1 and add>sum:
                    sum=add
            mo-=1

        return  sum

    def get_num(self,fram):

        user_last_day=fram.groupby('user_id',as_index=False)['day'].max().rename(columns={"day": "user_last_launch"})          #  求得最后登录的时间
        user_simer=fram.groupby('user_id',as_index=False).count().rename(columns={'day':'app_two_out'})                  # 统计重复出现次数
        user_mean_day=user_simer.copy()
        user_mean_day['app_two_out']=user_simer['app_two_out'].div(30).rename(columns={"app_two_out":'app_launch_mean'})      ## 统计每天行为数的平均数
        user_day_sum=fram.groupby(by='day',as_index=False).count().rename(columns={"user_id":'sum_user'})   ##每天有多少用户登录
        day_sum_user=user_day_sum.copy()
        day_sum_user['sum_user']=day_sum_user['sum_user'].div(len(fram))   ##每天平均登录多少用户
        day_sum_user=day_sum_user.rename(columns={'sum_user':'mean_user'})
        user_day_var=user_day_sum['sum_user'].var(),user_day_sum['sum_user'].std()     ##  方差 标准差

        fram=(fram[['user_id','day']]).drop_duplicates(['user_id','day'])

        continue_day=(fram.groupby('user_id',as_index=False).agg(lambda x:self.cal_day(x))).rename(columns={'day':'cont_day'})['cont_day']

        user_sum = pd.concat([user_last_day, user_mean_day["app_two_out"], day_sum_user['mean_user'], user_day_sum,pd.DataFrame([user_day_var[0]], columns=['var_user']),pd.DataFrame([user_day_var[1]], columns=['std_user']),continue_day], axis=1, join='outer')

        return  user_sum


    def get_num_activity(self,x):
        sum=0;
        for i in x:
            if i==0 or i==1 or i ==2:
                sum+=1
        return sum

    def get_num_activity(self,datafram,fram):

        action_0_1_2=fram[fram.action_type.isin([0]) |fram.action_type.isin([1]) |fram.action_type.isin([2]) ].groupby('user_id',as_index=False).count()['action_type']
        action_type_sum=(fram.drop_duplicates(['user_id','action_type'])[['user_id','action_type']].groupby(by=['user_id'],as_index=False).count())['action_type']
        action_page_type=fram[fram.action_type.isin(['0']) & fram.page.isin(['1'])].groupby('user_id',as_index=False).count()['page']
        action_28_29_30=fram[fram.day.isin(['28']) | fram.day.isin(['29']) | fram.day.isin(['30'])| fram.action_type.isin(['0'])].groupby('user_id',as_index=False).count()['day']

        datafram['action_0_1_2']=action_0_1_2
        datafram['action_type_sum']=action_type_sum
        datafram['action_page_type']=action_page_type
        datafram['action_28_29_30']=action_28_29_30

        return datafram

        pass


    def get_num_app_launch(self):
        address="C:\\Users\\ASUS\\Documents\\Tencent Files\\1223482107\\FileRecv\\app_launch_log.HDF"
        fram = self.open_file(address)
        datafram=self.get_num(fram)
        return  datafram

    def get_num_user_register(self):
        address="C:\\Users\\ASUS\\Documents\\Tencent Files\\1223482107\\FileRecv\\user_register_log.HDF"
        framer=self.open_file(address).rename(columns={"day": "user_register"})
        return  framer[["user_id","user_register"]]

    def get_num_user_activity(self):
        address="C:\\Users\\ASUS\\Documents\\Tencent Files\\1223482107\\FileRecv\\user_activity_log.HDF"
        fram=self.open_file(address)
        datafram=self.get_num(fram[['user_id','day']])
        datafram=self.get_num_activity(datafram,fram)  #####  测试 测试 测试
        return  datafram


     ## .groupby(by='user_id',as_index=False).count().rename(columns={"day": "video_count"})
    def get_num_video_create(self):
        address="C:\\Users\\ASUS\\Documents\\Tencent Files\\1223482107\\FileRecv\\video_create_log.HDF"
        fram=self.open_file(address)
        datafram=self.get_num(fram)
        return  datafram


    def user_last_register(self,begin,end):

        modo=pd.merge(begin,end,on='user_id')
        modo['last_regist']=modo['user_last_launch']-modo['user_register']
        return  modo[['user_id','last_regist']]

    def data_register_count(self,frame):

        return frame.groupby("user_register").count()

        pass


    def add_modo(self,data):

        group = data.groupby('user_register')
        group = pd.concat([pd.DataFrame(list(group.groups.keys()), columns=['last_rigist_day']),
                           group['user_id'].count().reset_index(drop=True)], axis=1)
        group.rename(columns={'user_id': 'day_count'}, inplace=True)
        return  group

    # 把提取出的数据汇总然后存入文件
    def make_list_to_sample(self):

         data_register=self.get_num_user_register()
         data_register['day_sum']=self.data_register_count(data_register)
         data_register.to_csv('user_register.csv')

         data_launch=self.get_num_app_launch()
         data_launch=pd.merge(data_launch,data_register,on='user_id')
         data_launch=pd.merge(data_launch,self.user_last_register(data_register[['user_id','user_register']],data_launch[['user_id','user_last_launch']]),on='user_id')
         data_launch=pd.concat([data_launch, self.add_modo(data_launch)], axis=1)
         data_launch.to_csv("app_launch.csv")


         data_video=self.get_num_video_create()
         data_video=pd.merge(data_video,data_register,on='user_id')
         data_video=pd.merge(data_video,self.user_last_register(data_register[['user_id','user_register']],data_video[['user_id','user_last_launch']]),on='user_id')
         data_video=pd.concat([data_video, self.add_modo(data_video)], axis=1)
         data_video.to_csv("video_create.csv")

         data_activity=self.get_num_user_activity()
         data_activity=pd.merge(data_activity,data_register,on='user_id')
         data_activity=pd.merge(data_activity,self.user_last_register(data_register[['user_id','user_register']],data_activity[['user_id','user_last_launch']]),on='user_id')
         data_activity=pd.concat([data_activity, self.add_modo(data_activity)], axis=1)
         data_activity.to_csv("user_activety.csv")


if __name__ == '__main__':

    begin=time.time()
    mod=modo()
    mod.make_list_to_sample()

    print(time.time()-begin)
    pass












