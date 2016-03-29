#/第一层时间划分方法***********************************************************************************************************************

Factor<-4.2
purchase<-0
cart<-0
collect<-1
click<-0.1
mydata3<-read.csv("t_alibaba_data.csv")#mydata3存原始数据
mydata3$visit_datetime<-as.Date(mydata3$visit_datetime)
#给purchase,cart,collect,click加上时间因素，前四个月分别乘以0.5,1,2,3.5
startdate<-as.Date("2013-04-15")
enddate<-as.Date("2013-05-15")
mydata3temp1<-mydata3[which(mydata3$visit_datetime>=startdate &
                      mydata3$visit_datetime<=enddate),]
mydata3temp1$score<-mydata3temp1$type
mydata3temp1$score[mydata3temp1$score==1]<-purchase*0.5
mydata3temp1$score[mydata3temp1$score==3]<-cart*0.5
mydata3temp1$score[mydata3temp1$score==2]<-collect*0.5
mydata3temp1$score[mydata3temp1$score==0]<-click*0.5
##fix(mydata3temp1)
startdate<-as.Date("2013-05-16")
enddate<-as.Date("2013-06-15")
mydata3temp2<-mydata3[which(mydata3$visit_datetime>=startdate &
                      mydata3$visit_datetime<=enddate),]
mydata3temp2$score<-mydata3temp2$type
mydata3temp2$score[mydata3temp2$score==1]<-purchase*1
mydata3temp2$score[mydata3temp2$score==3]<-cart*1
mydata3temp2$score[mydata3temp2$score==2]<-collect*1
mydata3temp2$score[mydata3temp2$score==0]<-click*1
startdate<-as.Date("2013-06-16")
enddate<-as.Date("2013-07-15")
mydata3temp3<-mydata3[which(mydata3$visit_datetime>=startdate &
                      mydata3$visit_datetime<=enddate),]
mydata3temp3$score<-mydata3temp3$type
mydata3temp3$score[mydata3temp3$score==1]<-purchase*2
mydata3temp3$score[mydata3temp3$score==3]<-cart*2
mydata3temp3$score[mydata3temp3$score==2]<-collect*2
mydata3temp3$score[mydata3temp3$score==0]<-click*2
startdate<-as.Date("2013-07-16")
enddate<-as.Date("2013-08-15")
mydata3temp4<-mydata3[which(mydata3$visit_datetime>=startdate &
                      mydata3$visit_datetime<=enddate),]
mydata3temp4$score<-mydata3temp4$type
mydata3temp4$score[mydata3temp4$score==1]<-purchase*3.5
mydata3temp4$score[mydata3temp4$score==3]<-cart*3.5
mydata3temp4$score[mydata3temp4$score==2]<-collect*3.5
mydata3temp4$score[mydata3temp4$score==0]<-click*3.5

mydata3temp1<-rbind(mydata3temp1,mydata3temp2)
mydata3temp1<-rbind(mydata3temp1,mydata3temp3)
mydata3temp1<-rbind(mydata3temp1,mydata3temp4)
##fix(mydata3)
mydata3<-mydata3temp1
##fix(mydata3)
#考虑给purchase,cart,collect,click加上时间因素后，对每个用户打分
len<-length(mydata3$user_id)#原始数据总的行数，len=182880
mydata3<-mydata3[order(mydata3$user_id),]#mydata3存按用户名升序排列后的数据
leveNum<-nlevels(as.factor(mydata3$user_id))#总的用户数，leveNum=884
index4<-duplicated(mydata3$user_id)
userID<-mydata3[!index4,c(1)]#884个具体的用户名(按升序排列)
mydata4<-data.frame(userID)#884*1的数据框，第一列为用户名(userID)
userNum<-function(userID)
    {k<-0
     temp<-rep(0,leveNum)#leveNum=884
     pre<-userID[1]
     leveTemp<-0
     for(i in 2:len)#len=182880
         if(pre==userID[i])
             leveTemp<-leveTemp+1
         else
             {k<-k+1;
              temp[k]<-leveTemp+1;
              pre<-userID[i];
              leveTemp<-0}
     temp[k+1]<-leveTemp+1
     return(temp)
    }
mydata4$user_num<-userNum(mydata3$user_id)#884个用户每个用户出现的总次数
#mydata4<-mydata4[order(-mydata4$user_num),]
##fix(mydata4)
for(i in 1:leveNum) {#leveNum=884
mydata5<-mydata3[which(mydata3$user_id==mydata4$userID[i]),]#i改为1

mydata4$user_scores[i]<-sum(mydata5$score)
}
mydata4<-mydata4[order(-mydata4$user_scores),]
#fix(mydata4)
#考虑给purchase,cart,collect,click加上时间因素后，每个用户对每个品牌的喜爱程度
for(i in 1:leveNum) {#884改为leveNum
mydata<-mydata3[which(mydata3$user_id==mydata4$userID[i]),]#i改为1
mydata<-mydata[order(mydata$brand_id),]
index1<-duplicated(mydata$brand_id)
num<-length(mydata[!index1,c(2)])
UserID<-rep(mydata$user_id[1],num)
mydata2<-data.frame(UserID)
mydata2$brand<-mydata[!index1,c(2)]
brandScore <- function(brandID,score,record_num)
    {k<-0
     scoreList<-rep(0,record_num)
     pre<-brandID[1]
     j<-1
     scoreTemp<-0
     if(record_num>=2)
        for(n in 2:record_num)
          if(pre==brandID[n])
             scoreTemp<-scoreTemp+score[n]
          else
            {k<-k+1;
             scoreList[k]<-score[j]+scoreTemp;
             pre<-brandID[n];
             j<-n;
             scoreTemp<-0
               }
     scoreList[k+1]<-score[j]+scoreTemp
     scoreList<-subset(scoreList,scoreList>0)
     return(scoreList)
    }
mydata2$totalScore<-brandScore(mydata$brand_id,mydata$score,mydata4$user_num[i])
mydata2<-mydata2[order(-mydata2$totalScore),]
if(i==1)
   {   write.table(mydata2[1,],"tmp.csv",sep = ",",quote = F,col.names =F,row.names =F)
       write.table(mydata2,"tmp.csv",sep = ",",quote = F,col.names =F,row.names =F,append=T)}
else
   write.table(mydata2,"tmp.csv",sep = ",",quote = F,col.names =F,row.names =F,append = T)
}
mydata6<-read.csv("tmp.csv",col.names=c("UserID","brand","totalScore"))

##fix(mydata6)
meanScore<-mean(mydata6$totalScore)*Factor#/*******************************************************************待调因子待调因子待调因子*****/
cat("平均分：",meanScore/Factor,"\t","阈值：",meanScore,"\n")
mydata6<-mydata6[which(mydata6$totalScore>meanScore),]
#fix(mydata6)
#mydata6<-mydata6[which(mydata6$totalScore>Factor),]
mydata6<-mydata6[,c(1,2)]
mydata61<-mydata6


#/第二层时间划分方法***********************************************************************************************************************

Factor<-4.2
purchase<-0
cart<-0
collect<-1
click<-0.1
mydata3<-read.csv("t_alibaba_data.csv")#mydata3存原始数据
mydata3$visit_datetime<-as.Date(mydata3$visit_datetime)
#给purchase,cart,collect,click加上时间因素，前四个月分别乘以0.5,1,2,3.5
startdate<-as.Date("2013-04-15")
enddate<-as.Date("2013-05-31")
mydata3temp1<-mydata3[which(mydata3$visit_datetime>=startdate &
                      mydata3$visit_datetime<=enddate),]
mydata3temp1$score<-mydata3temp1$type
mydata3temp1$score[mydata3temp1$score==1]<-purchase*0.5
mydata3temp1$score[mydata3temp1$score==3]<-cart*0.5
mydata3temp1$score[mydata3temp1$score==2]<-collect*0.5
mydata3temp1$score[mydata3temp1$score==0]<-click*0.5
##fix(mydata3temp1)
startdate<-as.Date("2013-06-01")
enddate<-as.Date("2013-06-30")
mydata3temp2<-mydata3[which(mydata3$visit_datetime>=startdate &
                      mydata3$visit_datetime<=enddate),]
mydata3temp2$score<-mydata3temp2$type
mydata3temp2$score[mydata3temp2$score==1]<-purchase*1
mydata3temp2$score[mydata3temp2$score==3]<-cart*1
mydata3temp2$score[mydata3temp2$score==2]<-collect*1
mydata3temp2$score[mydata3temp2$score==0]<-click*1
startdate<-as.Date("2013-07-01")
enddate<-as.Date("2013-07-31")
mydata3temp3<-mydata3[which(mydata3$visit_datetime>=startdate &
                      mydata3$visit_datetime<=enddate),]
mydata3temp3$score<-mydata3temp3$type
mydata3temp3$score[mydata3temp3$score==1]<-purchase*2
mydata3temp3$score[mydata3temp3$score==3]<-cart*2
mydata3temp3$score[mydata3temp3$score==2]<-collect*2
mydata3temp3$score[mydata3temp3$score==0]<-click*2
startdate<-as.Date("2013-08-01")
enddate<-as.Date("2013-08-15")
mydata3temp4<-mydata3[which(mydata3$visit_datetime>=startdate &
                      mydata3$visit_datetime<=enddate),]
mydata3temp4$score<-mydata3temp4$type
mydata3temp4$score[mydata3temp4$score==1]<-purchase*3.5
mydata3temp4$score[mydata3temp4$score==3]<-cart*3.5
mydata3temp4$score[mydata3temp4$score==2]<-collect*3.5
mydata3temp4$score[mydata3temp4$score==0]<-click*3.5

mydata3temp1<-rbind(mydata3temp1,mydata3temp2)
mydata3temp1<-rbind(mydata3temp1,mydata3temp3)
mydata3temp1<-rbind(mydata3temp1,mydata3temp4)
##fix(mydata3)
mydata3<-mydata3temp1
##fix(mydata3)
#考虑给purchase,cart,collect,click加上时间因素后，对每个用户打分
len<-length(mydata3$user_id)#原始数据总的行数，len=182880
mydata3<-mydata3[order(mydata3$user_id),]#mydata3存按用户名升序排列后的数据
leveNum<-nlevels(as.factor(mydata3$user_id))#总的用户数，leveNum=884
index4<-duplicated(mydata3$user_id)
userID<-mydata3[!index4,c(1)]#884个具体的用户名(按升序排列)
mydata4<-data.frame(userID)#884*1的数据框，第一列为用户名(userID)
userNum<-function(userID)
    {k<-0
     temp<-rep(0,leveNum)#leveNum=884
     pre<-userID[1]
     leveTemp<-0
     for(i in 2:len)#len=182880
         if(pre==userID[i])
             leveTemp<-leveTemp+1
         else
             {k<-k+1;
              temp[k]<-leveTemp+1;
              pre<-userID[i];
              leveTemp<-0}
     temp[k+1]<-leveTemp+1
     return(temp)
    }
mydata4$user_num<-userNum(mydata3$user_id)#884个用户每个用户出现的总次数
#mydata4<-mydata4[order(-mydata4$user_num),]
##fix(mydata4)
for(i in 1:leveNum) {#leveNum=884
mydata5<-mydata3[which(mydata3$user_id==mydata4$userID[i]),]#i改为1

mydata4$user_scores[i]<-sum(mydata5$score)
}
mydata4<-mydata4[order(-mydata4$user_scores),]
#fix(mydata4)
#考虑给purchase,cart,collect,click加上时间因素后，每个用户对每个品牌的喜爱程度
for(i in 1:leveNum) {#884改为leveNum
mydata<-mydata3[which(mydata3$user_id==mydata4$userID[i]),]#i改为1
mydata<-mydata[order(mydata$brand_id),]
index1<-duplicated(mydata$brand_id)
num<-length(mydata[!index1,c(2)])
UserID<-rep(mydata$user_id[1],num)
mydata2<-data.frame(UserID)
mydata2$brand<-mydata[!index1,c(2)]
brandScore <- function(brandID,score,record_num)
    {k<-0
     scoreList<-rep(0,record_num)
     pre<-brandID[1]
     j<-1
     scoreTemp<-0
     if(record_num>=2)
        for(n in 2:record_num)
          if(pre==brandID[n])
             scoreTemp<-scoreTemp+score[n]
          else
            {k<-k+1;
             scoreList[k]<-score[j]+scoreTemp;
             pre<-brandID[n];
             j<-n;
             scoreTemp<-0
               }
     scoreList[k+1]<-score[j]+scoreTemp
     scoreList<-subset(scoreList,scoreList>0)
     return(scoreList)
    }
mydata2$totalScore<-brandScore(mydata$brand_id,mydata$score,mydata4$user_num[i])
mydata2<-mydata2[order(-mydata2$totalScore),]
if(i==1)
   {   write.table(mydata2[1,],"tmp.csv",sep = ",",quote = F,col.names =F,row.names =F)
       write.table(mydata2,"tmp.csv",sep = ",",quote = F,col.names =F,row.names =F,append=T)}
else
   write.table(mydata2,"tmp.csv",sep = ",",quote = F,col.names =F,row.names =F,append = T)
}
mydata6<-read.csv("tmp.csv",col.names=c("UserID","brand","totalScore"))

##fix(mydata6)
meanScore<-mean(mydata6$totalScore)*Factor#/*******************************************************************待调因子待调因子待调因子*****/
cat("平均分：",meanScore/Factor,"\t","阈值:",meanScore,"\n")
mydata6<-mydata6[which(mydata6$totalScore>meanScore),]
#fix(mydata6)
#mydata6<-mydata6[which(mydata6$totalScore>Factor),]
mydata6<-mydata6[,c(1,2)]


#/考虑附加因素的影响*****************************************************************************************************



#/四个月每个用户购买的品牌及相应的个数，选出购买数超过2次的品牌**********************************************************************************


mydata<-read.csv("t_alibaba_data.csv")
mydata$visit_datetime<-as.Date(mydata$visit_datetime)
startdate<-as.Date("2013-04-15")
enddate<-as.Date("2013-08-15")
mydata1<-mydata[which(mydata$visit_datetime>=startdate &
                      mydata$visit_datetime<=enddate),]
mydata1purchase<-mydata1[which(mydata1$type==1),]
mydata1purchase<-mydata1purchase[order(mydata1purchase$user_id),]
index<-duplicated(mydata1purchase$user_id)
userID1<-mydata1purchase[!index,c(1)]
for(i in 1:length(userID1)) {
if(i==1) {
    brandID1<-mydata1purchase[which(mydata1purchase$user_id==userID1[i]),c(2)]
    len1<-length(brandID1)
    brandID1<-brandID1[order(brandID1)]
    index<-duplicated(brandID1)
    brandIDtemp<-brandID1[!index]
    n1<-length(brandIDtemp)

    userIDtemp<-rep(userID1[i],n1)
    mydata1purchasetemp<-data.frame(userIDtemp,brandIDtemp)

    brand1Num<-function(brandID)
    {k<-0
     temp<-rep(0,n1)
     pre<-brandID[1]
     leveTemp<-0
     if(len1>=2)
     for(j in 2:len1)
         if(pre==brandID[j])
             leveTemp<-leveTemp+1
         else
             {k<-k+1;
              temp[k]<-leveTemp+1;
              pre<-brandID[j];
              leveTemp<-0}
     temp[k+1]<-leveTemp+1
     return(temp)
    }
mydata1purchasetemp$brand_num<-brand1Num(brandID1)}
else
{ brandID1<-mydata1purchase[which(mydata1purchase$user_id==userID1[i]),c(2)]
    len1<-length(brandID1)
    brandID1<-brandID1[order(brandID1)]
    index<-duplicated(brandID1)
    brandIDtemp<-brandID1[!index]
    n1<-length(brandIDtemp)

    userIDtemp<-rep(userID1[i],n1)
    mydata1purchasetemp1<-data.frame(userIDtemp,brandIDtemp)

    brand1Num<-function(brandID)
    {k<-0
     temp<-rep(0,n1)
     pre<-brandID[1]
     leveTemp<-0
     if(len1>=2)
       for(j in 2:len1)
           if(pre==brandID[j])
               leveTemp<-leveTemp+1
           else
               {k<-k+1;
                temp[k]<-leveTemp+1;
                pre<-brandID[j];
                leveTemp<-0}
     temp[k+1]<-leveTemp+1
     return(temp)
    }
mydata1purchasetemp1$brand_num<-brand1Num(brandID1)
mydata1purchasetemp<-rbind(mydata1purchasetemp,mydata1purchasetemp1)
}}
mydata1purchasetemp<-mydata1purchasetemp[order(-mydata1purchasetemp$brand_num),]
mydatapurchaseAll<-mydata1purchasetemp
#str(mydatapurchaseAll)
#fix(mydatapurchaseAll)
mydatapurchaseAll<-mydatapurchaseAll[which(mydatapurchaseAll$brand_num>=2),c(1,2)]
names(mydatapurchaseAll)[names(mydatapurchaseAll)=="userIDtemp"]="UserID"
names(mydatapurchaseAll)[names(mydatapurchaseAll)=="brandIDtemp"]="brand"
str(mydatapurchaseAll)
#fix(mydatapurchaseAll)
#合并项*********************
mydata6<-rbind(mydata6,mydatapurchaseAll)#附加因素4个月购买数超过2次的品牌



#/考虑热销商品的影响,具体见lj7.R*****************************************************************
#第4个月下半个月热销前8品牌，最后1天点击但前4个月都没购买过的， 下面的brandtop这个变量的值需要自己写个
#小程序先求出来，然后把求出的前8个品牌写在下面的这个位置。

brandtop<-c(11196,504,155,25333,19862,15019,20979,27117)
mydata<-read.csv("t_alibaba_data.csv")
mydata$visit_datetime<-as.Date(mydata$visit_datetime)
###fix(mydata)
startdate<-as.Date("2013-08-15")
enddate<-as.Date("2013-08-15")
mydatabrandtopclick<-mydata[which(mydata$visit_datetime>=startdate &
                      mydata$visit_datetime<=enddate & mydata$type==0),]

for(i in 1:length(brandtop)) {
if(i==1) {
    userID1<-mydatabrandtopclick[which(mydatabrandtopclick$brand_id==brandtop[i]),c(1)]
    len1<-length(userID1)
    userID1<-userID1[order(userID1)]
    index<-duplicated(userID1)
    UserID<-userID1[!index]
    n1<-length(UserID)

    brand<-rep(brandtop[i],n1)
    mydatabrandtopclicktemp<-data.frame(UserID,brand)}

else
{ userID1<-mydatabrandtopclick[which(mydatabrandtopclick$brand_id==brandtop[i]),c(1)]
    len1<-length(userID1)
    userID1<-userID1[order(userID1)]
    index<-duplicated(userID1)
    UserID<-userID1[!index]
    n1<-length(UserID)

    brand<-rep(brandtop[i],n1)
    mydatabrandtopclicktemp1<-data.frame(UserID,brand)

mydatabrandtopclicktemp<-rbind(mydatabrandtopclicktemp,mydatabrandtopclicktemp1)
}}
mydatabrandtopclicktemp<-mydatabrandtopclicktemp[order(mydatabrandtopclicktemp$UserID),]
str(mydatabrandtopclicktemp)
#fix(mydatabrandtopclicktemp)

#/合并项********************
mydata6<-rbind(mydata6,mydatabrandtopclicktemp)#附加因素第4个月下半个月热销前8品牌，最后1天点击的

#/7.1-8.15加入购物车的用户和品牌***************************************************************

mydata<-read.csv("t_alibaba_data.csv")
mydata$visit_datetime<-as.Date(mydata$visit_datetime)   #/***************************新加
startdate<-as.Date("2013-07-01")
enddate<-as.Date("2013-08-15")
mydata1<-mydata[which(mydata$visit_datetime>=startdate &
                      mydata$visit_datetime<=enddate),]
mydata1cart<-mydata1[which(mydata1$type==3),]
mydata1cart<-mydata1cart[order(mydata1cart$user_id),]
index<-duplicated(mydata1cart$user_id)
User_id<-mydata1cart[!index,c(1)]

for(i in 1:length(User_id)) {

if(i==1) {
    brand_idtemp<-mydata1cart[which(mydata1cart$user_id==User_id[i]),c(2)]
index<-duplicated(brand_idtemp)
brand<-brand_idtemp[!index]
UserID<-rep(User_id[i],length(brand))
mydata1carttemp<-data.frame(UserID,brand)}
else
{brand_idtemp<-mydata1cart[which(mydata1cart$user_id==User_id[i]),c(2)]
index<-duplicated(brand_idtemp)
brand<-brand_idtemp[!index]
UserID<-rep(User_id[i],length(brand))
mydata1carttemp1<-data.frame(UserID,brand)
mydata1carttemp<-rbind(mydata1carttemp,mydata1carttemp1)}}
#str(mydata1carttemp)
###fix(mydata1carttemp)
#合并项*************************
mydata6<-rbind(mydata6,mydata1carttemp)#附加因素7.1-8.15加入购物车的品牌和用户

#/把4个月加入收藏2次或2次以上的品牌选出来**************************************

mydata<-read.csv("t_alibaba_data.csv")
mydata$visit_datetime<-as.Date(mydata$visit_datetime)   #/***************************新加
#前4阶段每个用户收藏的品牌及各品牌的个数
startdate<-as.Date("2013-04-15")
enddate<-as.Date("2013-08-15")
mydata1<-mydata[which(mydata$visit_datetime>=startdate &
                      mydata$visit_datetime<=enddate),]
mydata1purchase<-mydata1[which(mydata1$type==2),]
mydata1purchase<-mydata1purchase[order(mydata1purchase$user_id),]
index<-duplicated(mydata1purchase$user_id)
userID1<-mydata1purchase[!index,c(1)]
for(i in 1:length(userID1)) {
if(i==1) {
    brandID1<-mydata1purchase[which(mydata1purchase$user_id==userID1[i]),c(2)]
    len1<-length(brandID1)
    brandID1<-brandID1[order(brandID1)]
    index<-duplicated(brandID1)
    brandIDtemp<-brandID1[!index]
    n1<-length(brandIDtemp)

    userIDtemp<-rep(userID1[i],n1)
    mydata1purchasetemp<-data.frame(userIDtemp,brandIDtemp)

    brand1Num<-function(brandID)
    {k<-0
     temp<-rep(0,n1)
     pre<-brandID[1]
     leveTemp<-0
     if(len1>=2)
     for(j in 2:len1)
         if(pre==brandID[j])
             leveTemp<-leveTemp+1
         else
             {k<-k+1;
              temp[k]<-leveTemp+1;
              pre<-brandID[j];
              leveTemp<-0}
     temp[k+1]<-leveTemp+1
     return(temp)
    }
mydata1purchasetemp$brand_num<-brand1Num(brandID1)}
else
{ brandID1<-mydata1purchase[which(mydata1purchase$user_id==userID1[i]),c(2)]
    len1<-length(brandID1)
    brandID1<-brandID1[order(brandID1)]
    index<-duplicated(brandID1)
    brandIDtemp<-brandID1[!index]
    n1<-length(brandIDtemp)

    userIDtemp<-rep(userID1[i],n1)
    mydata1purchasetemp1<-data.frame(userIDtemp,brandIDtemp)

    brand1Num<-function(brandID)
    {k<-0
     temp<-rep(0,n1)
     pre<-brandID[1]
     leveTemp<-0
     if(len1>=2)
       for(j in 2:len1)
           if(pre==brandID[j])
               leveTemp<-leveTemp+1
           else
               {k<-k+1;
                temp[k]<-leveTemp+1;
                pre<-brandID[j];
                leveTemp<-0}
     temp[k+1]<-leveTemp+1
     return(temp)
    }
mydata1purchasetemp1$brand_num<-brand1Num(brandID1)
mydata1purchasetemp<-rbind(mydata1purchasetemp,mydata1purchasetemp1)
}}
mydata1purchasetempclick<-mydata1purchasetemp
mydata1purchasetempclick<-mydata1purchasetempclick[order(-mydata1purchasetempclick$brand_num),]
mydata1collectdouble<-mydata1purchasetempclick[which(mydata1purchasetempclick$brand_num>=2),c(1,2)]
names(mydata1collectdouble)[names(mydata1collectdouble)=="userIDtemp"]="UserID"
names(mydata1collectdouble)[names(mydata1collectdouble)=="brandIDtemp"]="brand"
str(mydata1collectdouble)
#fix(mydata1collectdouble)
#/合并项*************
mydata6<-rbind(mydata6,mydata1collectdouble)#附加因素4个月加入收藏2次或2次以上的品牌

#/前4个月购买1次且加入收藏1次的品牌**********************************************************

mydata<-read.csv("t_alibaba_data.csv")
mydata$visit_datetime<-as.Date(mydata$visit_datetime)
#前4阶段每个用户收藏的品牌及各品牌的个数
startdate<-as.Date("2013-04-15")
enddate<-as.Date("2013-08-15")
mydata1<-mydata[which(mydata$visit_datetime>=startdate &
                      mydata$visit_datetime<=enddate),]
mydata1purchase<-mydata1[which(mydata1$type==2),]
mydata1purchase<-mydata1purchase[order(mydata1purchase$user_id),]
index<-duplicated(mydata1purchase$user_id)
userID1<-mydata1purchase[!index,c(1)]
for(i in 1:length(userID1)) {
if(i==1) {
    brandID1<-mydata1purchase[which(mydata1purchase$user_id==userID1[i]),c(2)]
    len1<-length(brandID1)
    brandID1<-brandID1[order(brandID1)]
    index<-duplicated(brandID1)
    brandIDtemp<-brandID1[!index]
    n1<-length(brandIDtemp)

    userIDtemp<-rep(userID1[i],n1)
    mydata1purchasetemp<-data.frame(userIDtemp,brandIDtemp)

    brand1Num<-function(brandID)
    {k<-0
     temp<-rep(0,n1)
     pre<-brandID[1]
     leveTemp<-0
     if(len1>=2)
     for(j in 2:len1)
         if(pre==brandID[j])
             leveTemp<-leveTemp+1
         else
             {k<-k+1;
              temp[k]<-leveTemp+1;
              pre<-brandID[j];
              leveTemp<-0}
     temp[k+1]<-leveTemp+1
     return(temp)
    }
mydata1purchasetemp$brand_num<-brand1Num(brandID1)}
else
{ brandID1<-mydata1purchase[which(mydata1purchase$user_id==userID1[i]),c(2)]
    len1<-length(brandID1)
    brandID1<-brandID1[order(brandID1)]
    index<-duplicated(brandID1)
    brandIDtemp<-brandID1[!index]
    n1<-length(brandIDtemp)

    userIDtemp<-rep(userID1[i],n1)
    mydata1purchasetemp1<-data.frame(userIDtemp,brandIDtemp)

    brand1Num<-function(brandID)
    {k<-0
     temp<-rep(0,n1)
     pre<-brandID[1]
     leveTemp<-0
     if(len1>=2)
       for(j in 2:len1)
           if(pre==brandID[j])
               leveTemp<-leveTemp+1
           else
               {k<-k+1;
                temp[k]<-leveTemp+1;
                pre<-brandID[j];
                leveTemp<-0}
     temp[k+1]<-leveTemp+1
     return(temp)
    }
mydata1purchasetemp1$brand_num<-brand1Num(brandID1)
mydata1purchasetemp<-rbind(mydata1purchasetemp,mydata1purchasetemp1)
}}
mydata1purchasetempclick<-mydata1purchasetemp
mydata1purchasetempclick<-mydata1purchasetempclick[order(-mydata1purchasetempclick$brand_num),]
mydata1collectdoublemore<-mydata1purchasetempclick[which(mydata1purchasetempclick$brand_num==1),c(1,2)]
names(mydata1collectdoublemore)[names(mydata1collectdoublemore)=="userIDtemp"]="UserID"
names(mydata1collectdoublemore)[names(mydata1collectdoublemore)=="brandIDtemp"]="brand"
str(mydata1collectdoublemore)
#fix(mydata1collectdoublemore)
#/前4个月购买的品牌和个数
startdate<-as.Date("2013-04-15")
enddate<-as.Date("2013-08-15")
mydata1<-mydata[which(mydata$visit_datetime>=startdate &
                      mydata$visit_datetime<=enddate),]
mydata1purchase<-mydata1[which(mydata1$type==1),]
mydata1purchase<-mydata1purchase[order(mydata1purchase$user_id),]
index<-duplicated(mydata1purchase$user_id)
userID1<-mydata1purchase[!index,c(1)]
for(i in 1:length(userID1)) {
if(i==1) {
    brandID1<-mydata1purchase[which(mydata1purchase$user_id==userID1[i]),c(2)]
    len1<-length(brandID1)
    brandID1<-brandID1[order(brandID1)]
    index<-duplicated(brandID1)
    brandIDtemp<-brandID1[!index]
    n1<-length(brandIDtemp)

    userIDtemp<-rep(userID1[i],n1)
    mydata1purchasetemp<-data.frame(userIDtemp,brandIDtemp)

    brand1Num<-function(brandID)
    {k<-0
     temp<-rep(0,n1)
     pre<-brandID[1]
     leveTemp<-0
     if(len1>=2)
     for(j in 2:len1)
         if(pre==brandID[j])
             leveTemp<-leveTemp+1
         else
             {k<-k+1;
              temp[k]<-leveTemp+1;
              pre<-brandID[j];
              leveTemp<-0}
     temp[k+1]<-leveTemp+1
     return(temp)
    }
mydata1purchasetemp$brand_num<-brand1Num(brandID1)}
else
{ brandID1<-mydata1purchase[which(mydata1purchase$user_id==userID1[i]),c(2)]
    len1<-length(brandID1)
    brandID1<-brandID1[order(brandID1)]
    index<-duplicated(brandID1)
    brandIDtemp<-brandID1[!index]
    n1<-length(brandIDtemp)

    userIDtemp<-rep(userID1[i],n1)
    mydata1purchasetemp1<-data.frame(userIDtemp,brandIDtemp)

    brand1Num<-function(brandID)
    {k<-0
     temp<-rep(0,n1)
     pre<-brandID[1]
     leveTemp<-0
     if(len1>=2)
       for(j in 2:len1)
           if(pre==brandID[j])
               leveTemp<-leveTemp+1
           else
               {k<-k+1;
                temp[k]<-leveTemp+1;
                pre<-brandID[j];
                leveTemp<-0}
     temp[k+1]<-leveTemp+1
     return(temp)
    }
mydata1purchasetemp1$brand_num<-brand1Num(brandID1)
mydata1purchasetemp<-rbind(mydata1purchasetemp,mydata1purchasetemp1)
}}
mydata1purchasetemppurchase<-mydata1purchasetemp
mydata1purchasetemppurchase<-mydata1purchasetemppurchase[order(-mydata1purchasetemppurchase$brand_num),]
mydata1purchasetemppurchase<-mydata1purchasetemppurchase[which(mydata1purchasetemppurchase$brand_num==1),c(1,2)]
names(mydata1purchasetemppurchase)[names(mydata1purchasetemppurchase)=="userIDtemp"]="UserID"
names(mydata1purchasetemppurchase)[names(mydata1purchasetemppurchase)=="brandIDtemp"]="brand"
str(mydata1purchasetemppurchase)
#fix(mydata1purchasetemppurchase)
#/前4个月收藏1次且购买一次的品牌
index<-duplicated(mydata1collectdoublemore$UserID)
UserIDcollect<-mydata1collectdoublemore$UserID[!index]
index<-duplicated(mydata1purchasetemppurchase$UserID)
UserIDpurchase<-mydata1purchasetemppurchase$UserID[!index]
UserIDcopur<-intersect(UserIDcollect,UserIDpurchase)
for(i in 1:length(UserIDcopur)) {
if(i==1) {
brandcollectdouble<-mydata1collectdoublemore[which(mydata1collectdoublemore$UserID==UserIDcopur[i]),c(2)]
brandpurchasedouble<-mydata1purchasetemppurchase[which(mydata1purchasetemppurchase$UserID==UserIDcopur[i]),c(2)]
brandcopur<-intersect(brandcollectdouble,brandpurchasedouble)
UserIDcopurtemp<-rep(UserIDcopur[i],length(brandcopur))
mydatacopur<-data.frame(UserIDcopurtemp,brandcopur)}
else
    {brandcollectdouble<-mydata1collectdoublemore[which(mydata1collectdoublemore$UserID==UserIDcopur[i]),c(2)]
brandpurchasedouble<-mydata1purchasetemppurchase[which(mydata1purchasetemppurchase$UserID==UserIDcopur[i]),c(2)]
brandcopur<-intersect(brandcollectdouble,brandpurchasedouble)
UserIDcopurtemp<-rep(UserIDcopur[i],length(brandcopur))
mydatacopur1<-data.frame(UserIDcopurtemp,brandcopur)
 mydatacopur<-rbind(mydatacopur,mydatacopur1)}}
names(mydatacopur)[names(mydatacopur)=="UserIDcopurtemp"]="UserID"
names(mydatacopur)[names(mydatacopur)=="brandcopur"]="brand"
str(mydatacopur)
#fix(mydatacopur)
#/合并项***************************
mydata6<-rbind(mydata6,mydatacopur)#附加因素4个月购买1次且加入收藏1次的品牌



#/**********************************************************mydata6是接口********************************************************



#/***************************************数据框合并***********************************************************************

mydata6<-rbind(mydata6,mydata61)#mydata61为第1层时间划分中权重阈值选择mydata6
str(mydata6)
##fix(mydata6)
#/*********************************************************提交系统****************************************************
#/***********************************************************mydata6中去重******************************************************************
mydata6<-mydata6[order(mydata6$UserID),]
index6<-duplicated(mydata6$UserID)
USERID<-mydata6[!index6,c(1)]
Num<-length(USERID)
cat("推荐的用户数：",Num,"\n")
for(i in 1:Num) {
if(i==1) {
mydata71<-mydata6[which(mydata6$UserID==USERID[i]),]
index<-duplicated(mydata71$brand)
perBrand1<-mydata71$brand[!index]
USERID71<-rep(USERID[i],length(perBrand1))
mydata71temp<-data.frame(USERID71,perBrand1)}
else
    {mydata71<-mydata6[which(mydata6$UserID==USERID[i]),]
index<-duplicated(mydata71$brand)
perBrand1<-mydata71$brand[!index]
USERID71<-rep(USERID[i],length(perBrand1))
mydata71temp1<-data.frame(USERID71,perBrand1)
mydata71temp<-rbind(mydata71temp,mydata71temp1) }}
names(mydata71temp)[names(mydata71temp)=="USERID71"]="UserID"
names(mydata71temp)[names(mydata71temp)=="perBrand1"]="brand"
mydata6<-mydata71temp
lengBrand<-0
for(i in 1:Num) {
mydata7<-mydata6[which(mydata6$UserID==USERID[i]),]
perBrand<-mydata7$brand
lengBrand<-lengBrand+length(perBrand)
perBrand<-t(perBrand)
cat(USERID[i],file="result.txt",append = TRUE)
cat("\t",file="result.txt",append = TRUE)
cat(perBrand,file="result.txt",sep = ",",append = TRUE)
cat("\n",file="result.txt",append = TRUE)}
cat("推荐的品牌数：",lengBrand,"\n")

