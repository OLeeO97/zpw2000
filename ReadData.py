import numpy as np
import pandas as pd
import re
import os
from multiprocessing import Process

class DataProcessor(object):
    def __init__(self):
        pass

    #读取bytes文件
    def read_data(self,filepath):
        print('读取数据转为list...')
        with open (filepath,'rb') as f:
            f_hex = f.read().hex()
        group_count = int(len(f_hex) / 4 / 100)   #计算有多少路信号
        
        
        #每４位切分
        def cut_text(text,lenth):
            textArr = re.findall('.{'+str(lenth)+'}', text)
            textArr.append(text[(len(textArr)*lenth):])
            return textArr
        
        #交换高低位
        def change_bytes(text):
            return eval('0x'+text[2]+text[3]+text[0]+text[1])
        
        data_list = cut_text(f_hex,4)    #只读取前54个数据
        data_list.pop()   #多了一个''，弹出
        data_list = [change_bytes(i) for i in data_list]
        print('数据转list完成,共有{}路(每路100个模拟量)数据'.format(group_count))
        
        return data_list, group_count


    def to_float(self,ADValue,TypeCode):
        if((ADValue >= 0xfff0)&(ADValue<=0xffff)):
            return 99999;
        
        fValue = 0.0
        fADvalue = 0.0
        if (TypeCode >= 100):
            fValue = ADValue*0.1
        
        else:
            wOut = ADValue&0x6000
            wExp = wOut >> 13
        
            if (wExp == 0x01):
                fADCoeff = 0.01
            elif (wExp == 0x02):
                fADCoeff = 0.1
            elif (wExp == 0x00):
                fADCoeff == 1
            elif (wExp == 0x03):
                fADCoeff  =10

            wOut = ADValue&0x1fff
            fValue = wOut*fADCoeff

            if((ADValue&0x8000)==0x8000):
                fValue*=-1

        return fValue

    #读取数据用pandas保存
    def saveAsCSV(self,data_list,rows,file):
        print('list数据转dataframe...')
        data_np = np.zeros([rows,100])
        for row in range(rows):
            for col in range(100):
                data_np[row,col] = data_list[col+row*100]

        moniliang = ['功出电压1[172-182]','功出电流1[329-429]','功出载频1','功出低频1',
             '送端电缆侧电压1[151-158]','送端电缆侧电流1[173-377]','送端电缆侧载频1','送端电缆侧低频1',
             '受端电缆侧主轨电压1[3.8-8.7]','受端电缆侧主轨载频1','受端电缆侧主轨低频1',
             '受端电缆侧小轨电压1[500-641]','受端电缆侧小轨载频1','受端电缆侧小轨低频1',
             '受端设备侧主轨电压1[1711-2520]','受端设备侧主轨载频1','受端设备侧主轨低频1',
             '受端设备侧小轨电压1[113.1-219.5]','受端设备侧小轨载频1','受端设备侧小轨低频1',
             '接收入口主轨电压1[270-396.9]','接收入口主轨载频1','接收入口主轨低频1',
             '接收入口小轨电压1[145-165]','接收入口小轨载频1','接收入口小轨低频1','道床1',
             '功出电压2[172-182]','功出电流2[329-429]','功出载频2','功出低频2',
             '送端电缆侧电压2[151-158]','送端电缆侧电流2[173-377]','送端电缆侧载频2','送端电缆侧低频2',
             '受端电缆侧主轨电压2[3.8-8.7]','受端电缆侧主轨载频2','受端电缆侧主轨低频2',
             '受端电缆侧小轨电压2[500-641]','受端电缆侧小轨载频2','受端电缆侧小轨低频2',
             '受端设备侧主轨电压2[1711-2520]','受端设备侧主轨载频2','受端设备侧主轨低频2',
             '受端设备侧小轨电压2[113.1-219.5]','受端设备侧小轨载频2','受端设备侧小轨低频2',
             '接收入口主轨电压2[270-396.9]','接收入口主轨载频2','接收入口主轨低频2',
             '接收入口小轨电压2[145-165]','接收入口小轨载频2','接收入口小轨低频2','道床2',
             '功出电压3[172-182]','功出电流3[329-429]','功出载频3','功出低频3',
             '送端电缆侧电压3[151-158]','送端电缆侧电流3','送端电缆侧载频3','送端电缆侧低频3',
             '受端电缆侧主轨电压3[3.8-8.7]','受端电缆侧主轨载频3','受端电缆侧主轨低频3',
             '受端电缆侧小轨电压3[500-641]','受端电缆侧小轨载频3','受端电缆侧小轨低频3',
             '受端设备侧主轨电压3[1711-2520]','受端设备侧主轨载频3','受端设备侧主轨低频3',
             '受端设备侧小轨电压3[113.1-219.5]','受端设备侧小轨载频3','受端设备侧小轨低频3',
             '接收入口主轨电压3[270-396.9]','接收入口主轨载频3','接收入口主轨低频3',
             '接收入口小轨电压3[145-165]','接收入口小轨载频3','接收入口小轨低频3','道床3',
             '功出电压4[172-182]','功出电流4[329-429]','功出载频4','功出低频4',
             '送端电缆侧电压4[151-158]','送端电缆侧电流4','送端电缆侧载频4','送端电缆侧低频4',
             '受端电缆侧主轨电压4[3.8-8.7]','受端电缆侧主轨载频4','受端电缆侧主轨低频4',
             '受端电缆侧小轨电压4[500-641]','受端电缆侧小轨载频4','受端电缆侧小轨低频4',
             '受端设备侧主轨电压4[1711-2520]','受端设备侧主轨载频4','受端设备侧主轨低频4',
             '受端设备侧小轨电压4[113.1-219.5]','受端设备侧小轨载频4'
             ]

        data_df = pd.DataFrame(data=data_np)
        print('list数据转dataframe完成．．．')
        data_df.to_csv(str(file[:-3]+'csv'),header=moniliang)

    def main(self,filepath,file):
        data_list, group_count = self.read_data(filepath+file)
        data_list = [self.to_float(i,160) for i in data_list]
        self.saveAsCSV(data_list,group_count,file)

if __name__ == '__main__':
    dp = DataProcessor()
    filepath = '../historydata/2020-04-16/mnldata/'
    files = os.listdir(filepath)
    for file in files:
        if file.startswith('160') and not os.path.exites(file[:10]+'csv'):
            print('转换文件　{}'.format(file))
            dp.main(filepath,file)
        

    
            
