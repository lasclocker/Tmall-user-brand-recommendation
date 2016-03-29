
#计算提交的品牌个数:
#select sum(regexp_count(brand,',')+1) from t_tmall_add_user_brand_predict_dh;
  
#/*********************************************************************


sql("""create table wx1_offer_train_5_27_table1 as
     select user_id,brand_id,count(brand_id) as clickNum
     from source_data
     where type=0 and visit_datetime>='04-15' and visit_datetime<='07-15'
     group by user_id,brand_id""");
	 
sql("""create table wx1_offer_train_5_27_table2 as
     select user_id,brand_id,count(brand_id) as buyNum
     from source_data
     where type=1 and visit_datetime>='04-15' and visit_datetime<='07-15'
     group by user_id,brand_id""");

sql("""create table wx1_offer_train_5_27_table3 as
     select user_id,brand_id,count(brand_id) as collectNum
     from source_data
     where type=2 and visit_datetime>='04-15' and visit_datetime<='07-15'
     group by user_id,brand_id""");

sql("""create table wx1_offer_train_5_27_table4 as
     select user_id,brand_id,count(brand_id) as cartNum
     from source_data
     where type=3 and visit_datetime>='04-15' and visit_datetime<='07-15'
     group by user_id,brand_id""");

sql("""create table wx1_offer_train_5_27_table5 as
     select user_id,brand_id,1 as buyYN
     from source_data
     where type=1 and visit_datetime>='07-16' and visit_datetime<='08-15'
     group by user_id,brand_id""");
	 
sql("""create table quan_wx1_offer_train_5_27_table1 as
     select user_id,brand_id
     from source_data
     where visit_datetime>='04-15' and visit_datetime<='07-15'
     group by user_id,brand_id""");
	 
sql("""create table wx1_offer_train_5_27_table as
       select a.user_id,a.brand_id,b.clickNum,c.buyNum,d.collectNum,e.cartNum,f.buyYN
       from quan_wx1_offer_train_5_27_table1 a 
       left outer join wx1_offer_train_5_27_table1 b 
       on a.user_id=b.user_id and a.brand_id=b.brand_id
	   left outer join wx1_offer_train_5_27_table2 c
       on a.user_id=c.user_id and a.brand_id=c.brand_id
       left outer join wx1_offer_train_5_27_table3 d
       on a.user_id=d.user_id and a.brand_id=d.brand_id
       left outer join wx1_offer_train_5_27_table4 e
       on a.user_id=e.user_id and a.brand_id=e.brand_id
       left outer join wx1_offer_train_5_27_table5 f
       on a.user_id=f.user_id and a.brand_id=f.brand_id
    """);
#/wx1_offer_train_5_27_table填充缺失值后为:wx1_offer_train_5_27_table123
configs = [('clickNum', 'null', '0'),('buyNum', 'null', '0'), ('collectNum', 'null', '0'),('cartNum', 'null', '0'),         
		  ('buyYN', 'null', '0')]
DataProc.fillMissingValues('wx1_offer_train_5_27_table', 'wx1_offer_train_5_27_table123', configs);
	
sql("""create table wx1_offer_april_may_train_5_27_table1 as
     select user_id,brand_id,count(brand_id) as clickNum1
     from source_data
     where type=0 and visit_datetime>='04-15' and visit_datetime<='05-15'
     group by user_id,brand_id""");
sql("""create table wx1_offer_april_may_train_5_27_table2 as
     select user_id,brand_id,count(brand_id) as buyNum1
     from source_data
     where type=1 and visit_datetime>='04-15' and visit_datetime<='05-15'
     group by user_id,brand_id""");
sql("""create table wx1_offer_april_may_train_5_27_table3 as
     select user_id,brand_id,count(brand_id) as collectNum1
     from source_data
     where type=2 and visit_datetime>='04-15' and visit_datetime<='05-15'
     group by user_id,brand_id""");
sql("""create table wx1_offer_april_may_train_5_27_table4 as
     select user_id,brand_id,count(brand_id) as cartNum1
     from source_data
     where type=3 and visit_datetime>='04-15' and visit_datetime<='05-15'
     group by user_id,brand_id""");
	 
sql("""create table wx1_offer_may_june_train_5_27_table1 as
     select user_id,brand_id,count(brand_id) as clickNum2
     from source_data
     where type=0 and visit_datetime>='05-16' and visit_datetime<='06-15'
     group by user_id,brand_id""");
sql("""create table wx1_offer_may_june_train_5_27_table2 as
     select user_id,brand_id,count(brand_id) as buyNum2
     from source_data
     where type=1 and visit_datetime>='05-16' and visit_datetime<='06-15'
     group by user_id,brand_id""");
sql("""create table wx1_offer_may_june_train_5_27_table3 as
     select user_id,brand_id,count(brand_id) as collectNum2
     from source_data
     where type=2 and visit_datetime>='05-16' and visit_datetime<='06-15'
     group by user_id,brand_id""");
sql("""create table wx1_offer_may_june_train_5_27_table4 as
     select user_id,brand_id,count(brand_id) as cartNum2
     from source_data
     where type=3 and visit_datetime>='05-16' and visit_datetime<='06-15'
     group by user_id,brand_id""");
	 
sql("""create table wx1_offer_june_july_train_5_27_table1 as
     select user_id,brand_id,count(brand_id) as clickNum3
     from source_data
     where type=0 and visit_datetime>='06-16' and visit_datetime<='07-15'
     group by user_id,brand_id""");
sql("""create table wx1_offer_june_july_train_5_27_table2 as
     select user_id,brand_id,count(brand_id) as buyNum3
     from source_data
     where type=1 and visit_datetime>='06-16' and visit_datetime<='07-15'
     group by user_id,brand_id""");
sql("""create table wx1_offer_june_july_train_5_27_table3 as
     select user_id,brand_id,count(brand_id) as collectNum3
     from source_data
     where type=2 and visit_datetime>='06-16' and visit_datetime<='07-15'
     group by user_id,brand_id""");
sql("""create table wx1_offer_june_july_train_5_27_table4 as
     select user_id,brand_id,count(brand_id) as cartNum3
     from source_data
     where type=3 and visit_datetime>='06-16' and visit_datetime<='07-15'
     group by user_id,brand_id""");
	 

sql("""create table wx1_offer_train_5_27_table_time as
       select a.user_id,a.brand_id,a.clickNum,a.buyNum,a.collectNum,a.cartNum,a.buyYN,m.clickNum1,b.buyNum1,c.collectNum1,d.cartNum1,
	   e.clickNum2,f.buyNum2,g.collectNum2,h.cartNum2,i.clickNum3,j.buyNum3,k.collectNum3,l.cartNum3
       from wx1_offer_train_5_27_table123 a
	   left outer join wx1_offer_april_may_train_5_27_table1 m 
	   on a.user_id=m.user_id and a.brand_id=m.brand_id
       left outer join wx1_offer_april_may_train_5_27_table2 b 
       on a.user_id=b.user_id and a.brand_id=b.brand_id
       left outer join wx1_offer_april_may_train_5_27_table3 c
       on a.user_id=c.user_id and a.brand_id=c.brand_id
       left outer join wx1_offer_april_may_train_5_27_table4 d
       on a.user_id=d.user_id and a.brand_id=d.brand_id
       left outer join wx1_offer_may_june_train_5_27_table1 e
       on a.user_id=e.user_id and a.brand_id=e.brand_id
	   left outer join wx1_offer_may_june_train_5_27_table2 f 
       on a.user_id=f.user_id and a.brand_id=f.brand_id
       left outer join wx1_offer_may_june_train_5_27_table3 g
       on a.user_id=g.user_id and a.brand_id=g.brand_id
       left outer join wx1_offer_may_june_train_5_27_table4 h
       on a.user_id=h.user_id and a.brand_id=h.brand_id
	   left outer join wx1_offer_june_july_train_5_27_table1 i
	   on a.user_id=i.user_id and a.brand_id=i.brand_id
	   left outer join wx1_offer_june_july_train_5_27_table2 j
	   on a.user_id=j.user_id and a.brand_id=j.brand_id
	   left outer join wx1_offer_june_july_train_5_27_table3 k
	   on a.user_id=k.user_id and a.brand_id=k.brand_id
	   left outer join wx1_offer_june_july_train_5_27_table4 l
	   on a.user_id=l.user_id and a.brand_id=l.brand_id
    """);

#/wx1_offer_train_5_27_table_time填充缺失值后为:wx1_offer_train_5_27_table_time123
configs = [('clickNum1', 'null', '0'),('buyNum1', 'null', '0'), ('collectNum1', 'null', '0'),('cartNum1', 'null', '0'),        
		  ('clickNum2', 'null', '0'),('buyNum2', 'null', '0'), ('collectNum2', 'null', '0'),('cartNum2', 'null', '0'),
		  ('clickNum3', 'null', '0'),('buyNum3', 'null', '0'), ('collectNum3', 'null', '0'),('cartNum3', 'null', '0')         
		  ]
DataProc.fillMissingValues('wx1_offer_train_5_27_table_time', 'wx1_offer_train_5_27_table_time123', configs);
#/***************************************************************************************

sql("""create table wx1_offer_predict_5_27_table1 as
     select user_id,brand_id,count(brand_id) as clickNum
     from source_data
     where type=0 and visit_datetime>='05-16' and visit_datetime<='08-15'
     group by user_id,brand_id""");

sql("""create table wx1_offer_predict_5_27_table2 as
     select user_id,brand_id,count(brand_id) as buyNum
     from source_data
     where type=1 and visit_datetime>='05-16' and visit_datetime<='08-15'
     group by user_id,brand_id""");

sql("""create table wx1_offer_predict_5_27_table3 as
     select user_id,brand_id,count(brand_id) as collectNum
     from source_data
     where type=2 and visit_datetime>='05-16' and visit_datetime<='08-15'
     group by user_id,brand_id""");

sql("""create table wx1_offer_predict_5_27_table4 as
     select user_id,brand_id,count(brand_id) as cartNum
     from source_data
     where type=3 and visit_datetime>='05-16' and visit_datetime<='08-15'
     group by user_id,brand_id""");
	 
sql("""create table quan_wx1_offer_predict_5_27_table1 as
     select user_id,brand_id
     from source_data
     where visit_datetime>='05-16' and visit_datetime<='08-15'
     group by user_id,brand_id""");
	 
sql("""create table wx1_offer_predict_5_27_table as
       select a.user_id,a.brand_id,b.clickNum,c.buyNum,d.collectNum,e.cartNum
       from quan_wx1_offer_predict_5_27_table1 a 
       left outer join wx1_offer_predict_5_27_table1 b 
       on a.user_id=b.user_id and a.brand_id=b.brand_id
	   left outer join wx1_offer_predict_5_27_table2 c
       on a.user_id=c.user_id and a.brand_id=c.brand_id
       left outer join wx1_offer_predict_5_27_table3 d
       on a.user_id=d.user_id and a.brand_id=d.brand_id
       left outer join wx1_offer_predict_5_27_table4 e
       on a.user_id=e.user_id and a.brand_id=e.brand_id
    """);
#/wx1_offer_predict_5_27_table填充缺失值后为:wx1_offer_predict_5_27_table123
configs = [('clickNum', 'null', '0'),('buyNum', 'null', '0'), ('collectNum', 'null', '0'),('cartNum', 'null', '0')]
DataProc.fillMissingValues('wx1_offer_predict_5_27_table', 'wx1_offer_predict_5_27_table123', configs);

sql("""create table wx1_offer_may_june_predict_5_27_table1 as
     select user_id,brand_id,count(brand_id) as clickNum1
     from source_data
     where type=0 and visit_datetime>='05-16' and visit_datetime<='06-15'
     group by user_id,brand_id""");
sql("""create table wx1_offer_may_june_predict_5_27_table2 as
     select user_id,brand_id,count(brand_id) as buyNum1
     from source_data
     where type=1 and visit_datetime>='05-16' and visit_datetime<='06-15'
     group by user_id,brand_id""");
sql("""create table wx1_offer_may_june_predict_5_27_table3 as
     select user_id,brand_id,count(brand_id) as collectNum1
     from source_data
     where type=2 and visit_datetime>='05-16' and visit_datetime<='06-15'
     group by user_id,brand_id""");
sql("""create table wx1_offer_may_june_predict_5_27_table4 as
     select user_id,brand_id,count(brand_id) as cartNum1
     from source_data
     where type=3 and visit_datetime>='05-16' and visit_datetime<='06-15'
     group by user_id,brand_id""");
	 
sql("""create table wx1_offer_june_july_predict_5_27_table1 as
     select user_id,brand_id,count(brand_id) as clickNum2
     from source_data
     where type=0 and visit_datetime>='06-16' and visit_datetime<='07-15'
     group by user_id,brand_id""");
sql("""create table wx1_offer_june_july_predict_5_27_table2 as
     select user_id,brand_id,count(brand_id) as buyNum2
     from source_data
     where type=1 and visit_datetime>='06-16' and visit_datetime<='07-15'
     group by user_id,brand_id""");
sql("""create table wx1_offer_june_july_predict_5_27_table3 as
     select user_id,brand_id,count(brand_id) as collectNum2
     from source_data
     where type=2 and visit_datetime>='06-16' and visit_datetime<='07-15'
     group by user_id,brand_id""");
sql("""create table wx1_offer_june_july_predict_5_27_table4 as
     select user_id,brand_id,count(brand_id) as cartNum2
     from source_data
     where type=3 and visit_datetime>='06-16' and visit_datetime<='07-15'
     group by user_id,brand_id""");

sql("""create table wx1_offer_july_august_predict_5_27_table1 as
     select user_id,brand_id,count(brand_id) as clickNum3
     from source_data
     where type=0 and visit_datetime>='07-16' and visit_datetime<='08-15'
     group by user_id,brand_id""");
sql("""create table wx1_offer_july_august_predict_5_27_table2 as
     select user_id,brand_id,count(brand_id) as buyNum3
     from source_data
     where type=1 and visit_datetime>='07-16' and visit_datetime<='08-15'
     group by user_id,brand_id""");
sql("""create table wx1_offer_july_august_predict_5_27_table3 as
     select user_id,brand_id,count(brand_id) as collectNum3
     from source_data
     where type=2 and visit_datetime>='07-16' and visit_datetime<='08-15'
     group by user_id,brand_id""");
sql("""create table wx1_offer_july_august_predict_5_27_table4 as
     select user_id,brand_id,count(brand_id) as cartNum3
     from source_data
     where type=3 and visit_datetime>='07-16' and visit_datetime<='08-15'
     group by user_id,brand_id""");

sql("""create table wx1_offer_predict_5_27_table_time as
	   select a.user_id,a.brand_id,a.clickNum,a.buyNum,a.collectNum,a.cartNum,m.clickNum1,b.buyNum1,c.collectNum1,d.cartNum1,
	   e.clickNum2,f.buyNum2,g.collectNum2,h.cartNum2,i.clickNum3,j.buyNum3,k.collectNum3,l.cartNum3
       from wx1_offer_predict_5_27_table123 a
	   left outer join wx1_offer_may_june_predict_5_27_table1 m 
	   on a.user_id=m.user_id and a.brand_id=m.brand_id 
       left outer join wx1_offer_may_june_predict_5_27_table2 b  	   
       on a.user_id=b.user_id and a.brand_id=b.brand_id
       left outer join wx1_offer_may_june_predict_5_27_table3 c
       on a.user_id=c.user_id and a.brand_id=c.brand_id
       left outer join wx1_offer_may_june_predict_5_27_table4 d
       on a.user_id=d.user_id and a.brand_id=d.brand_id
       left outer join wx1_offer_june_july_predict_5_27_table1 e
       on a.user_id=e.user_id and a.brand_id=e.brand_id
	   left outer join wx1_offer_june_july_predict_5_27_table2 f 
       on a.user_id=f.user_id and a.brand_id=f.brand_id
       left outer join wx1_offer_june_july_predict_5_27_table3 g
       on a.user_id=g.user_id and a.brand_id=g.brand_id
       left outer join wx1_offer_june_july_predict_5_27_table4 h
       on a.user_id=h.user_id and a.brand_id=h.brand_id
	   left outer join wx1_offer_july_august_predict_5_27_table1 i
	   on a.user_id=i.user_id and a.brand_id=i.brand_id
	   left outer join wx1_offer_july_august_predict_5_27_table2 j
	   on a.user_id=j.user_id and a.brand_id=j.brand_id
	   left outer join wx1_offer_july_august_predict_5_27_table3 k
	   on a.user_id=k.user_id and a.brand_id=k.brand_id
	   left outer join wx1_offer_july_august_predict_5_27_table4 l
	   on a.user_id=l.user_id and a.brand_id=l.brand_id
    """);
#/wx1_offer_predict_5_27_table_time填充缺失值后为:wx1_offer_predict_5_27_table_time123

configs = [('clickNum1', 'null', '0'),('buyNum1', 'null', '0'), ('collectNum1', 'null', '0'),('cartNum1', 'null', '0'),        
		  ('clickNum2', 'null', '0'),('buyNum2', 'null', '0'), ('collectNum2', 'null', '0'),('cartNum2', 'null', '0'),
		  ('clickNum3', 'null', '0'),('buyNum3', 'null', '0'), ('collectNum3', 'null', '0'),('cartNum3', 'null', '0')         
		  ]
DataProc.fillMissingValues('wx1_offer_predict_5_27_table_time', 'wx1_offer_predict_5_27_table_time123', configs);
#/*****************************
#/把最后1、3、7、10、15天用户-品牌点击数统计出来(训练):
sql("""create table wx1_5_16_1_last_one_days_train_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as last1_clickNum
     from source_data
     where type=0 and visit_datetime>='07-15' and visit_datetime<='07-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_three_days_train_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as last3_clickNum
     from source_data
     where type=0 and visit_datetime>='07-13' and visit_datetime<='07-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_seven_days_train_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as last7_clickNum
     from source_data
     where type=0 and visit_datetime>='07-09' and visit_datetime<='07-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_ten_days_train_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as last10_clickNum
     from source_data
     where type=0 and visit_datetime>='07-06' and visit_datetime<='07-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_fifteen_days_train_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as last15_clickNum
     from source_data
     where type=0 and visit_datetime>='07-01' and visit_datetime<='07-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_3_train_5_27_table_data123_quan123 as 
       select  a.user_id,a.brand_id,a.clickNum,a.buyNum,a.collectNum,a.cartNum,a.buyYN,
	           a.clickNum1,a.buyNum1,a.collectNum1,a.cartNum1,
			   a.clickNum2,a.buyNum2,a.collectNum2,a.cartNum2,
			   a.clickNum3,a.buyNum3,a.collectNum3,a.cartNum3,
			   b.last1_clicknum,c.last3_clickNum,d.last7_clickNum,e.last10_clickNum,f.last15_clickNum
	   from wx1_offer_train_5_27_table_time123 a
	   left outer join wx1_5_16_1_last_one_days_train_5_27_table_click1 b
	   on a.user_id=b.user_id and a.brand_id=b.brand_id
	   left outer join wx1_5_16_1_last_three_days_train_5_27_table_click1 c
	   on a.user_id=c.user_id and a.brand_id=c.brand_id
	   left outer join wx1_5_16_1_last_seven_days_train_5_27_table_click1 d
	   on a.user_id=d.user_id and a.brand_id=d.brand_id
	   left outer join wx1_5_16_1_last_ten_days_train_5_27_table_click1 e
	   on a.user_id=e.user_id and a.brand_id=e.brand_id
	   left outer join wx1_5_16_1_last_fifteen_days_train_5_27_table_click1 f
	   on a.user_id=f.user_id and a.brand_id=f.brand_id""");
configs = [('last1_clickNum', 'null', '0'),('last3_clickNum', 'null', '0'),('last7_clickNum', 'null', '0'),
            ('last10_clickNum', 'null', '0'),('last15_clickNum', 'null', '0')]
DataProc.fillMissingValues('wx1_5_16_3_train_5_27_table_data123_quan123', 'wx1_5_16_3_train_5_27_table_data123_quan12345', configs);
#/把最后1、3、7、10、15天用户-品牌购买数统计出来(训练):
sql("""create table wx1_5_16_1_last_one_days_train_5_27_table_buy1 as
     select user_id,brand_id,count(brand_id) as last1_buyNum
     from source_data
     where type=1 and visit_datetime>='07-15' and visit_datetime<='07-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_three_days_train_5_27_table_buy1 as
     select user_id,brand_id,count(brand_id) as last3_buyNum
     from source_data
     where type=1 and visit_datetime>='07-13' and visit_datetime<='07-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_seven_days_train_5_27_table_buy1 as
     select user_id,brand_id,count(brand_id) as last7_buyNum
     from source_data
     where type=1 and visit_datetime>='07-09' and visit_datetime<='07-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_ten_days_train_5_27_table_buy1 as
     select user_id,brand_id,count(brand_id) as last10_buyNum
     from source_data
     where type=1 and visit_datetime>='07-06' and visit_datetime<='07-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_fifteen_days_train_5_27_table_buy1 as
     select user_id,brand_id,count(brand_id) as last15_buyNum
     from source_data
     where type=1 and visit_datetime>='07-01' and visit_datetime<='07-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_17_1_train_5_27_table_data123_quan123 as 
       select  a.user_id,a.brand_id,a.clickNum,a.buyNum,a.collectNum,a.cartNum,a.buyYN,
	           a.clickNum1,a.buyNum1,a.collectNum1,a.cartNum1,
			   a.clickNum2,a.buyNum2,a.collectNum2,a.cartNum2,
			   a.clickNum3,a.buyNum3,a.collectNum3,a.cartNum3,
			   a.last1_clicknum,a.last3_clickNum,a.last7_clickNum,a.last10_clickNum,
			   a.last15_clickNum,b.last1_buyNum,c.last3_buyNum,d.last7_buyNum,e.last10_buyNum,f.last15_buyNum
	   from wx1_5_16_3_train_5_27_table_data123_quan12345 a
	   left outer join wx1_5_16_1_last_one_days_train_5_27_table_buy1 b
	   on a.user_id=b.user_id and a.brand_id=b.brand_id
	   left outer join wx1_5_16_1_last_three_days_train_5_27_table_buy1 c
	   on a.user_id=c.user_id and a.brand_id=c.brand_id
	   left outer join wx1_5_16_1_last_seven_days_train_5_27_table_buy1 d
	   on a.user_id=d.user_id and a.brand_id=d.brand_id
	   left outer join wx1_5_16_1_last_ten_days_train_5_27_table_buy1 e
	   on a.user_id=e.user_id and a.brand_id=e.brand_id
	   left outer join wx1_5_16_1_last_fifteen_days_train_5_27_table_buy1 f
	   on a.user_id=f.user_id and a.brand_id=f.brand_id""");
configs = [('last1_buyNum', 'null', '0'),('last3_buyNum', 'null', '0'),('last7_buyNum', 'null', '0'),
            ('last10_buyNum', 'null', '0'),('last15_buyNum', 'null', '0')]
DataProc.fillMissingValues('wx1_5_17_1_train_5_27_table_data123_quan123', 'wx1_5_17_1_train_5_27_table_data123_quan12345', configs);
#/把最后1、3、7、10、15天用户-品牌收藏数统计出来(训练):
sql("""create table wx1_5_16_1_last_one_days_train_5_27_table_collect1 as
     select user_id,brand_id,count(brand_id) as last1_collectNum
     from source_data
     where type=2 and visit_datetime>='07-15' and visit_datetime<='07-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_three_days_train_5_27_table_collect1 as
     select user_id,brand_id,count(brand_id) as last3_collectNum
     from source_data
     where type=2 and visit_datetime>='07-13' and visit_datetime<='07-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_seven_days_train_5_27_table_collect1 as
     select user_id,brand_id,count(brand_id) as last7_collectNum
     from source_data
     where type=2 and visit_datetime>='07-09' and visit_datetime<='07-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_ten_days_train_5_27_table_collect1 as
     select user_id,brand_id,count(brand_id) as last10_collectNum
     from source_data
     where type=2 and visit_datetime>='07-06' and visit_datetime<='07-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_fifteen_days_train_5_27_table_collect1 as
     select user_id,brand_id,count(brand_id) as last15_collectNum
     from source_data
     where type=2 and visit_datetime>='07-01' and visit_datetime<='07-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_17_2_train_5_27_table_data123_quan123 as 
       select  a.user_id,a.brand_id,a.clickNum,a.buyNum,a.collectNum,a.cartNum,a.buyYN,
	           a.clickNum1,a.buyNum1,a.collectNum1,a.cartNum1,
			   a.clickNum2,a.buyNum2,a.collectNum2,a.cartNum2,
			   a.clickNum3,a.buyNum3,a.collectNum3,a.cartNum3,
			   a.last1_clicknum,a.last3_clickNum,a.last7_clickNum,a.last10_clickNum,
			   a.last15_clickNum,a.last1_buyNum,a.last3_buyNum,a.last7_buyNum,a.last10_buyNum,
			   a.last15_buyNum,b.last1_collectNum,c.last3_collectNum,d.last7_collectNum,
			   e.last10_collectNum,f.last15_collectNum
	   from wx1_5_17_1_train_5_27_table_data123_quan12345 a
	   left outer join wx1_5_16_1_last_one_days_train_5_27_table_collect1 b
	   on a.user_id=b.user_id and a.brand_id=b.brand_id
	   left outer join wx1_5_16_1_last_three_days_train_5_27_table_collect1 c
	   on a.user_id=c.user_id and a.brand_id=c.brand_id
	   left outer join wx1_5_16_1_last_seven_days_train_5_27_table_collect1 d
	   on a.user_id=d.user_id and a.brand_id=d.brand_id
	   left outer join wx1_5_16_1_last_ten_days_train_5_27_table_collect1 e
	   on a.user_id=e.user_id and a.brand_id=e.brand_id
	   left outer join wx1_5_16_1_last_fifteen_days_train_5_27_table_collect1 f
	   on a.user_id=f.user_id and a.brand_id=f.brand_id""");
configs = [('last1_collectNum', 'null', '0'),('last3_collectNum', 'null', '0'),('last7_collectNum', 'null', '0'),
            ('last10_collectNum', 'null', '0'),('last15_collectNum', 'null', '0')]
DataProc.fillMissingValues('wx1_5_17_2_train_5_27_table_data123_quan123', 'wx1_5_17_2_train_5_27_table_data123_quan12345', configs);
#/把最后1、3、7、10、15天用户-品牌加购物车数统计出来(训练):
sql("""create table wx1_5_16_1_last_one_days_train_5_27_table_cart1 as
     select user_id,brand_id,count(brand_id) as last1_cartNum
     from source_data
     where type=3 and visit_datetime>='07-15' and visit_datetime<='07-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_three_days_train_5_27_table_cart1 as
     select user_id,brand_id,count(brand_id) as last3_cartNum
     from source_data
     where type=3 and visit_datetime>='07-13' and visit_datetime<='07-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_seven_days_train_5_27_table_cart1 as
     select user_id,brand_id,count(brand_id) as last7_cartNum
     from source_data
     where type=3 and visit_datetime>='07-09' and visit_datetime<='07-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_ten_days_train_5_27_table_cart1 as
     select user_id,brand_id,count(brand_id) as last10_cartNum
     from source_data
     where type=3 and visit_datetime>='07-06' and visit_datetime<='07-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_fifteen_days_train_5_27_table_cart1 as
     select user_id,brand_id,count(brand_id) as last15_cartNum
     from source_data
     where type=3 and visit_datetime>='07-01' and visit_datetime<='07-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_17_3_train_5_27_table_data123_quan123 as 
       select  a.user_id,a.brand_id,a.clickNum,a.buyNum,a.collectNum,a.cartNum,a.buyYN,
	           a.clickNum1,a.buyNum1,a.collectNum1,a.cartNum1,
			   a.clickNum2,a.buyNum2,a.collectNum2,a.cartNum2,
			   a.clickNum3,a.buyNum3,a.collectNum3,a.cartNum3,
			   a.last1_clicknum,a.last3_clickNum,a.last7_clickNum,a.last10_clickNum,
			   a.last15_clickNum,a.last1_buyNum,a.last3_buyNum,a.last7_buyNum,a.last10_buyNum,
			   a.last15_buyNum,a.last1_collectNum,a.last3_collectNum,a.last7_collectNum,
			   a.last10_collectNum,a.last15_collectNum,b.last1_cartNum,c.last3_cartNum,
			   d.last7_cartNum,e.last10_cartNum,f.last15_cartNum
	   from wx1_5_17_2_train_5_27_table_data123_quan12345 a
	   left outer join wx1_5_16_1_last_one_days_train_5_27_table_cart1 b
	   on a.user_id=b.user_id and a.brand_id=b.brand_id
	   left outer join wx1_5_16_1_last_three_days_train_5_27_table_cart1 c
	   on a.user_id=c.user_id and a.brand_id=c.brand_id
	   left outer join wx1_5_16_1_last_seven_days_train_5_27_table_cart1 d
	   on a.user_id=d.user_id and a.brand_id=d.brand_id
	   left outer join wx1_5_16_1_last_ten_days_train_5_27_table_cart1 e
	   on a.user_id=e.user_id and a.brand_id=e.brand_id
	   left outer join wx1_5_16_1_last_fifteen_days_train_5_27_table_cart1 f
	   on a.user_id=f.user_id and a.brand_id=f.brand_id""");
configs = [('last1_cartNum', 'null', '0'),('last3_cartNum', 'null', '0'),('last7_cartNum', 'null', '0'),
            ('last10_cartNum', 'null', '0'),('last15_cartNum', 'null', '0')]
DataProc.fillMissingValues('wx1_5_17_3_train_5_27_table_data123_quan123', 'wx1_5_17_3_train_5_27_table_data123_quan12345', configs);
#/把最后1、3、7、10、15天用户-品牌点击数统计出来(预测):
sql("""create table wx1_5_16_1_last_one_days_predict_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as last1_clickNum
     from source_data
     where type=0 and visit_datetime>='08-15' and visit_datetime<='08-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_three_days_predict_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as last3_clickNum
     from source_data
     where type=0 and visit_datetime>='08-13' and visit_datetime<='08-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_seven_days_predict_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as last7_clickNum
     from source_data
     where type=0 and visit_datetime>='08-09' and visit_datetime<='08-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_ten_days_predict_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as last10_clickNum
     from source_data
     where type=0 and visit_datetime>='08-06' and visit_datetime<='08-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_fifteen_days_predict_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as last15_clickNum
     from source_data
     where type=0 and visit_datetime>='08-01' and visit_datetime<='08-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_3_predict_5_27_table_data123_quan123 as 
       select  a.user_id,a.brand_id,a.clickNum,a.buyNum,a.collectNum,a.cartNum,
	           a.clickNum1,a.buyNum1,a.collectNum1,a.cartNum1,
			   a.clickNum2,a.buyNum2,a.collectNum2,a.cartNum2,
			   a.clickNum3,a.buyNum3,a.collectNum3,a.cartNum3,
			   b.last1_clicknum,c.last3_clickNum,d.last7_clickNum,e.last10_clickNum,f.last15_clickNum
	   from wx1_offer_predict_5_27_table_time123 a
	   left outer join wx1_5_16_1_last_one_days_predict_5_27_table_click1 b
	   on a.user_id=b.user_id and a.brand_id=b.brand_id
	   left outer join wx1_5_16_1_last_three_days_predict_5_27_table_click1 c
	   on a.user_id=c.user_id and a.brand_id=c.brand_id
	   left outer join wx1_5_16_1_last_seven_days_predict_5_27_table_click1 d
	   on a.user_id=d.user_id and a.brand_id=d.brand_id
	   left outer join wx1_5_16_1_last_ten_days_predict_5_27_table_click1 e
	   on a.user_id=e.user_id and a.brand_id=e.brand_id
	   left outer join wx1_5_16_1_last_fifteen_days_predict_5_27_table_click1 f
	   on a.user_id=f.user_id and a.brand_id=f.brand_id""");
configs = [('last1_clickNum', 'null', '0'),('last3_clickNum', 'null', '0'),('last7_clickNum', 'null', '0'),
            ('last10_clickNum', 'null', '0'),('last15_clickNum', 'null', '0')]
DataProc.fillMissingValues('wx1_5_16_3_predict_5_27_table_data123_quan123', 'wx1_5_16_3_predict_5_27_table_data123_quan12345', configs);
#/把最后1、3、7、10、15天用户-品牌购买数统计出来(预测):
sql("""create table wx1_5_16_1_last_one_days_predict_5_27_table_buy1 as
     select user_id,brand_id,count(brand_id) as last1_buyNum
     from source_data
     where type=1 and visit_datetime>='08-15' and visit_datetime<='08-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_three_days_predict_5_27_table_buy1 as
     select user_id,brand_id,count(brand_id) as last3_buyNum
     from source_data
     where type=1 and visit_datetime>='08-13' and visit_datetime<='08-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_seven_days_predict_5_27_table_buy1 as
     select user_id,brand_id,count(brand_id) as last7_buyNum
     from source_data
     where type=1 and visit_datetime>='08-09' and visit_datetime<='08-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_ten_days_predict_5_27_table_buy1 as
     select user_id,brand_id,count(brand_id) as last10_buyNum
     from source_data
     where type=1 and visit_datetime>='08-06' and visit_datetime<='08-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_fifteen_days_predict_5_27_table_buy1 as
     select user_id,brand_id,count(brand_id) as last15_buyNum
     from source_data
     where type=1 and visit_datetime>='08-01' and visit_datetime<='08-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_17_1_predict_5_27_table_data123_quan123 as 
       select  a.user_id,a.brand_id,a.clickNum,a.buyNum,a.collectNum,a.cartNum,
	           a.clickNum1,a.buyNum1,a.collectNum1,a.cartNum1,
			   a.clickNum2,a.buyNum2,a.collectNum2,a.cartNum2,
			   a.clickNum3,a.buyNum3,a.collectNum3,a.cartNum3,
			   a.last1_clicknum,a.last3_clickNum,a.last7_clickNum,a.last10_clickNum,
			   a.last15_clickNum,b.last1_buyNum,c.last3_buyNum,d.last7_buyNum,e.last10_buyNum,f.last15_buyNum
	   from wx1_5_16_3_predict_5_27_table_data123_quan12345 a
	   left outer join wx1_5_16_1_last_one_days_predict_5_27_table_buy1 b
	   on a.user_id=b.user_id and a.brand_id=b.brand_id
	   left outer join wx1_5_16_1_last_three_days_predict_5_27_table_buy1 c
	   on a.user_id=c.user_id and a.brand_id=c.brand_id
	   left outer join wx1_5_16_1_last_seven_days_predict_5_27_table_buy1 d
	   on a.user_id=d.user_id and a.brand_id=d.brand_id
	   left outer join wx1_5_16_1_last_ten_days_predict_5_27_table_buy1 e
	   on a.user_id=e.user_id and a.brand_id=e.brand_id
	   left outer join wx1_5_16_1_last_fifteen_days_predict_5_27_table_buy1 f
	   on a.user_id=f.user_id and a.brand_id=f.brand_id""");
configs = [('last1_buyNum', 'null', '0'),('last3_buyNum', 'null', '0'),('last7_buyNum', 'null', '0'),
            ('last10_buyNum', 'null', '0'),('last15_buyNum', 'null', '0')]
DataProc.fillMissingValues('wx1_5_17_1_predict_5_27_table_data123_quan123', 'wx1_5_17_1_predict_5_27_table_data123_quan12345', configs);
#/把最后1、3、7、10、15天用户-品牌收藏数统计出来(预测):
sql("""create table wx1_5_16_1_last_one_days_predict_5_27_table_collect1 as
     select user_id,brand_id,count(brand_id) as last1_collectNum
     from source_data
     where type=2 and visit_datetime>='08-15' and visit_datetime<='08-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_three_days_predict_5_27_table_collect1 as
     select user_id,brand_id,count(brand_id) as last3_collectNum
     from source_data
     where type=2 and visit_datetime>='08-13' and visit_datetime<='08-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_seven_days_predict_5_27_table_collect1 as
     select user_id,brand_id,count(brand_id) as last7_collectNum
     from source_data
     where type=2 and visit_datetime>='08-09' and visit_datetime<='08-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_ten_days_predict_5_27_table_collect1 as
     select user_id,brand_id,count(brand_id) as last10_collectNum
     from source_data
     where type=2 and visit_datetime>='08-06' and visit_datetime<='08-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_fifteen_days_predict_5_27_table_collect1 as
     select user_id,brand_id,count(brand_id) as last15_collectNum
     from source_data
     where type=2 and visit_datetime>='08-01' and visit_datetime<='08-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_17_2_predict_5_27_table_data123_quan123 as 
       select  a.user_id,a.brand_id,a.clickNum,a.buyNum,a.collectNum,a.cartNum,
	           a.clickNum1,a.buyNum1,a.collectNum1,a.cartNum1,
			   a.clickNum2,a.buyNum2,a.collectNum2,a.cartNum2,
			   a.clickNum3,a.buyNum3,a.collectNum3,a.cartNum3,
			   a.last1_clicknum,a.last3_clickNum,a.last7_clickNum,a.last10_clickNum,
			   a.last15_clickNum,a.last1_buyNum,a.last3_buyNum,a.last7_buyNum,a.last10_buyNum,
			   a.last15_buyNum,b.last1_collectNum,c.last3_collectNum,d.last7_collectNum,
			   e.last10_collectNum,f.last15_collectNum
	   from wx1_5_17_1_predict_5_27_table_data123_quan12345 a
	   left outer join wx1_5_16_1_last_one_days_predict_5_27_table_collect1 b
	   on a.user_id=b.user_id and a.brand_id=b.brand_id
	   left outer join wx1_5_16_1_last_three_days_predict_5_27_table_collect1 c
	   on a.user_id=c.user_id and a.brand_id=c.brand_id
	   left outer join wx1_5_16_1_last_seven_days_predict_5_27_table_collect1 d
	   on a.user_id=d.user_id and a.brand_id=d.brand_id
	   left outer join wx1_5_16_1_last_ten_days_predict_5_27_table_collect1 e
	   on a.user_id=e.user_id and a.brand_id=e.brand_id
	   left outer join wx1_5_16_1_last_fifteen_days_predict_5_27_table_collect1 f
	   on a.user_id=f.user_id and a.brand_id=f.brand_id""");
configs = [('last1_collectNum', 'null', '0'),('last3_collectNum', 'null', '0'),('last7_collectNum', 'null', '0'),
            ('last10_collectNum', 'null', '0'),('last15_collectNum', 'null', '0')]
DataProc.fillMissingValues('wx1_5_17_2_predict_5_27_table_data123_quan123', 'wx1_5_17_2_predict_5_27_table_data123_quan12345', configs);
#/把最后1、3、7、10、15天用户-品牌加购物车数统计出来(预测):
sql("""create table wx1_5_16_1_last_one_days_predict_5_27_table_cart1 as
     select user_id,brand_id,count(brand_id) as last1_cartNum
     from source_data
     where type=3 and visit_datetime>='08-15' and visit_datetime<='08-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_three_days_predict_5_27_table_cart1 as
     select user_id,brand_id,count(brand_id) as last3_cartNum
     from source_data
     where type=3 and visit_datetime>='08-13' and visit_datetime<='08-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_seven_days_predict_5_27_table_cart1 as
     select user_id,brand_id,count(brand_id) as last7_cartNum
     from source_data
     where type=3 and visit_datetime>='08-09' and visit_datetime<='08-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_ten_days_predict_5_27_table_cart1 as
     select user_id,brand_id,count(brand_id) as last10_cartNum
     from source_data
     where type=3 and visit_datetime>='08-06' and visit_datetime<='08-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_16_1_last_fifteen_days_predict_5_27_table_cart1 as
     select user_id,brand_id,count(brand_id) as last15_cartNum
     from source_data
     where type=3 and visit_datetime>='08-01' and visit_datetime<='08-15'
     group by user_id,brand_id""");
sql("""create table wx1_5_17_3_predict_5_27_table_data123_quan123 as 
       select  a.user_id,a.brand_id,a.clickNum,a.buyNum,a.collectNum,a.cartNum,
	           a.clickNum1,a.buyNum1,a.collectNum1,a.cartNum1,
			   a.clickNum2,a.buyNum2,a.collectNum2,a.cartNum2,
			   a.clickNum3,a.buyNum3,a.collectNum3,a.cartNum3,
			   a.last1_clicknum,a.last3_clickNum,a.last7_clickNum,a.last10_clickNum,
			   a.last15_clickNum,a.last1_buyNum,a.last3_buyNum,a.last7_buyNum,a.last10_buyNum,
			   a.last15_buyNum,a.last1_collectNum,a.last3_collectNum,a.last7_collectNum,
			   a.last10_collectNum,a.last15_collectNum,b.last1_cartNum,c.last3_cartNum,
			   d.last7_cartNum,e.last10_cartNum,f.last15_cartNum
	   from wx1_5_17_2_predict_5_27_table_data123_quan12345 a
	   left outer join wx1_5_16_1_last_one_days_predict_5_27_table_cart1 b
	   on a.user_id=b.user_id and a.brand_id=b.brand_id
	   left outer join wx1_5_16_1_last_three_days_predict_5_27_table_cart1 c
	   on a.user_id=c.user_id and a.brand_id=c.brand_id
	   left outer join wx1_5_16_1_last_seven_days_predict_5_27_table_cart1 d
	   on a.user_id=d.user_id and a.brand_id=d.brand_id
	   left outer join wx1_5_16_1_last_ten_days_predict_5_27_table_cart1 e
	   on a.user_id=e.user_id and a.brand_id=e.brand_id
	   left outer join wx1_5_16_1_last_fifteen_days_predict_5_27_table_cart1 f
	   on a.user_id=f.user_id and a.brand_id=f.brand_id""");
configs = [('last1_cartNum', 'null', '0'),('last3_cartNum', 'null', '0'),('last7_cartNum', 'null', '0'),
            ('last10_cartNum', 'null', '0'),('last15_cartNum', 'null', '0')]
DataProc.fillMissingValues('wx1_5_17_3_predict_5_27_table_data123_quan123', 'wx1_5_17_3_predict_5_27_table_data123_quan12345', configs);
#训练阶段，每个用户不同天对品牌的行为 
sql("""create table wx1_offer_per_click_train_5_19_5_27_table as 
       select user_id,brand_id,count(distinct visit_datetime) as diff_date_click_num
	   from source_data where type=0 
	   and visit_datetime>="04-15" and visit_datetime<="07-15"
	   group by user_id,brand_id""");
sql("""create table wx1_offer_per_buy_train_5_19_5_27_table as 
       select user_id,brand_id,count(distinct visit_datetime) as diff_date_buy_num
	   from source_data where type=1 
	   and visit_datetime>="04-15" and visit_datetime<="07-15"
	   group by user_id,brand_id""");
sql("""create table wx1_offer_per_collect_train_5_19_5_27_table as 
       select user_id,brand_id,count(distinct visit_datetime) as diff_date_collect_num
	   from source_data where type=2 
	   and visit_datetime>="04-15" and visit_datetime<="07-15"
	   group by user_id,brand_id""");
sql("""create table wx1_offer_per_cart_train_5_19_5_27_table as 
       select user_id,brand_id,count(distinct visit_datetime) as diff_date_cart_num
	   from source_data where type=3
	   and visit_datetime>="04-15" and visit_datetime<="07-15"
	   group by user_id,brand_id""");
sql("""create table wx1_5_19_3_train_5_19_5_27_table_data123_quan123 as 
       select  a.user_id,a.brand_id,a.clickNum,a.buyNum,a.collectNum,a.cartNum,a.buyYN,
	           a.clickNum1,a.buyNum1,a.collectNum1,a.cartNum1,
			   a.clickNum2,a.buyNum2,a.collectNum2,a.cartNum2,
			   a.clickNum3,a.buyNum3,a.collectNum3,a.cartNum3,
			   a.last1_clicknum,a.last3_clickNum,a.last7_clickNum,a.last10_clickNum,
			   a.last15_clickNum,a.last1_buyNum,a.last3_buyNum,a.last7_buyNum,a.last10_buyNum,
			   a.last15_buyNum,a.last1_collectNum,a.last3_collectNum,a.last7_collectNum,
			   a.last10_collectNum,a.last15_collectNum,a.last1_cartNum,a.last3_cartNum,
			   a.last7_cartNum,a.last10_cartNum,a.last15_cartNum,
               b.diff_date_click_num,c.diff_date_buy_num,d.diff_date_collect_num,e.diff_date_cart_num			   
	   from wx1_5_17_3_train_5_27_table_data123_quan12345 a
	   left outer join wx1_offer_per_click_train_5_19_5_27_table b
	   on a.user_id=b.user_id and a.brand_id=b.brand_id
	   left outer join wx1_offer_per_buy_train_5_19_5_27_table c
	   on a.user_id=c.user_id and a.brand_id=c.brand_id
	   left outer join wx1_offer_per_collect_train_5_19_5_27_table d
	   on a.user_id=d.user_id and a.brand_id=d.brand_id
	   left outer join wx1_offer_per_cart_train_5_19_5_27_table e
	   on a.user_id=e.user_id and a.brand_id=e.brand_id
	   """);
configs = [('diff_date_click_num', 'null', '0'),('diff_date_buy_num', 'null', '0'),
          ('diff_date_collect_num', 'null', '0'),('diff_date_cart_num', 'null', '0')]
DataProc.fillMissingValues('wx1_5_19_3_train_5_19_5_27_table_data123_quan123', 'wx1_5_19_3_train_5_19_5_27_table_data123_quan12345', configs);
#预测阶段，每个用户不同天对品牌的行为 
sql("""create table wx1_offer_per_click_predict_5_19_5_27_table as 
       select user_id,brand_id,count(distinct visit_datetime) as diff_date_click_num
	   from source_data where type=0 
	   and visit_datetime>="05-15" and visit_datetime<="08-15"
	   group by user_id,brand_id""");
sql("""create table wx1_offer_per_buy_predict_5_19_5_27_table as 
       select user_id,brand_id,count(distinct visit_datetime) as diff_date_buy_num
	   from source_data where type=1 
	   and visit_datetime>="05-15" and visit_datetime<="08-15"
	   group by user_id,brand_id""");
sql("""create table wx1_offer_per_collect_predict_5_19_5_27_table as 
       select user_id,brand_id,count(distinct visit_datetime) as diff_date_collect_num
	   from source_data where type=2 
	   and visit_datetime>="05-15" and visit_datetime<="08-15"
	   group by user_id,brand_id""");
sql("""create table wx1_offer_per_cart_predict_5_19_5_27_table as 
       select user_id,brand_id,count(distinct visit_datetime) as diff_date_cart_num
	   from source_data where type=3
	   and visit_datetime>="05-15" and visit_datetime<="08-15"
	   group by user_id,brand_id""");
sql("""create table wx1_5_19_3_predict_5_19_5_27_table_data123_quan123 as 
       select  a.user_id,a.brand_id,a.clickNum,a.buyNum,a.collectNum,a.cartNum,
	           a.clickNum1,a.buyNum1,a.collectNum1,a.cartNum1,
			   a.clickNum2,a.buyNum2,a.collectNum2,a.cartNum2,
			   a.clickNum3,a.buyNum3,a.collectNum3,a.cartNum3,
			   a.last1_clicknum,a.last3_clickNum,a.last7_clickNum,a.last10_clickNum,
			   a.last15_clickNum,a.last1_buyNum,a.last3_buyNum,a.last7_buyNum,a.last10_buyNum,
			   a.last15_buyNum,a.last1_collectNum,a.last3_collectNum,a.last7_collectNum,
			   a.last10_collectNum,a.last15_collectNum,a.last1_cartNum,a.last3_cartNum,
			   a.last7_cartNum,a.last10_cartNum,a.last15_cartNum,
               b.diff_date_click_num,c.diff_date_buy_num,d.diff_date_collect_num,e.diff_date_cart_num			   
	   from wx1_5_17_3_predict_5_27_table_data123_quan12345 a
	   left outer join wx1_offer_per_click_predict_5_19_5_27_table b
	   on a.user_id=b.user_id and a.brand_id=b.brand_id
	   left outer join wx1_offer_per_buy_predict_5_19_5_27_table c
	   on a.user_id=c.user_id and a.brand_id=c.brand_id
	   left outer join wx1_offer_per_collect_predict_5_19_5_27_table d
	   on a.user_id=d.user_id and a.brand_id=d.brand_id
	   left outer join wx1_offer_per_cart_predict_5_19_5_27_table e
	   on a.user_id=e.user_id and a.brand_id=e.brand_id
	   """);
configs = [('diff_date_click_num', 'null', '0'),('diff_date_buy_num', 'null', '0'),
          ('diff_date_collect_num', 'null', '0'),('diff_date_cart_num', 'null', '0')]
DataProc.fillMissingValues('wx1_5_19_3_predict_5_19_5_27_table_data123_quan123', 'wx1_5_19_3_predict_5_19_5_27_table_data123_quan12345', configs);

#/最后5天及21天点击
sql("""create table wx1_6_7_0_last_five_days_train_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as last5_clickNum
     from source_data
     where type=0 and visit_datetime>='07-11' and visit_datetime<='07-15'
     group by user_id,brand_id""");

sql("""create table wx1_6_7_0_last_twentyOne_days_train_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as last21_clickNum
     from source_data
     where type=0 and visit_datetime>='06-25' and visit_datetime<='07-15'
     group by user_id,brand_id""");
	 
sql("""create table wx1_6_7_0_last_firstOneTen_days_train_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as firstOneTen_clickNum
     from source_data
     where type=0 and visit_datetime>='04-15' and visit_datetime<='04-24'
     group by user_id,brand_id""");
	 
sql("""create table wx1_6_7_0_last_firstSecondTen_days_train_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as firstSecondTen_clickNum
     from source_data
     where type=0 and visit_datetime>='04-25' and visit_datetime<='05-04'
     group by user_id,brand_id""");
	 
sql("""create table wx1_6_7_0_last_firstThirdTen_days_train_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as firstThirdTen_clickNum
     from source_data
     where type=0 and visit_datetime>='05-05' and visit_datetime<='05-15'
     group by user_id,brand_id""");
	 
sql("""create table wx1_6_7_0_last_secondOneTen_days_train_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as secondOneTen_clickNum
     from source_data
     where type=0 and visit_datetime>='05-16' and visit_datetime<='05-25'
     group by user_id,brand_id""");
	 
sql("""create table wx1_6_7_0_last_secondSecondTen_days_train_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as secondSecondTen_clickNum
     from source_data
     where type=0 and visit_datetime>='05-26' and visit_datetime<='06-04'
     group by user_id,brand_id""");
	 
sql("""create table wx1_6_7_0_last_secondThirdTen_days_train_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as secondThirdTen_clickNum
     from source_data
     where type=0 and visit_datetime>='06-05' and visit_datetime<='06-15'
     group by user_id,brand_id""");
	 
sql("""create table wx1_6_7_0_train_5_19_5_27_table_data123_quan123 as 
       select  a.user_id,a.brand_id,a.clickNum,a.buyNum,a.collectNum,a.cartNum,a.buyYN,
	           a.clickNum1,a.buyNum1,a.collectNum1,a.cartNum1,
			   a.clickNum2,a.buyNum2,a.collectNum2,a.cartNum2,
			   a.clickNum3,a.buyNum3,a.collectNum3,a.cartNum3,
			   a.last1_clicknum,a.last3_clickNum,a.last7_clickNum,a.last10_clickNum,
			   a.last15_clickNum,a.last1_buyNum,a.last3_buyNum,a.last7_buyNum,a.last10_buyNum,
			   a.last15_buyNum,a.last1_collectNum,a.last3_collectNum,a.last7_collectNum,
			   a.last10_collectNum,a.last15_collectNum,a.last1_cartNum,a.last3_cartNum,
			   a.last7_cartNum,a.last10_cartNum,a.last15_cartNum,
               a.diff_date_click_num,a.diff_date_buy_num,a.diff_date_collect_num,a.diff_date_cart_num,
			   b.last5_clickNum,c.last21_clickNum,d.firstOneTen_clickNum,e.firstSecondTen_clickNum,
			   f.firstThirdTen_clickNum,g.secondOneTen_clickNum,h.secondSecondTen_clickNum,
			   i.secondThirdTen_clickNum
	   from wx1_5_19_3_train_5_19_5_27_table_data123_quan12345 a
	   left outer join wx1_6_7_0_last_five_days_train_5_27_table_click1 b
	   on a.user_id=b.user_id and a.brand_id=b.brand_id
	   left outer join wx1_6_7_0_last_twentyOne_days_train_5_27_table_click1 c
	   on a.user_id=c.user_id and a.brand_id=c.brand_id
	   left outer join wx1_6_7_0_last_firstOneTen_days_train_5_27_table_click1 d
	   on a.user_id=d.user_id and a.brand_id=d.brand_id
	   left outer join wx1_6_7_0_last_firstSecondTen_days_train_5_27_table_click1 e
	   on a.user_id=e.user_id and a.brand_id=e.brand_id
	   left outer join wx1_6_7_0_last_firstThirdTen_days_train_5_27_table_click1 f
	   on a.user_id=f.user_id and a.brand_id=f.brand_id
	   left outer join wx1_6_7_0_last_secondOneTen_days_train_5_27_table_click1 g
	   on a.user_id=g.user_id and a.brand_id=g.brand_id
	   left outer join wx1_6_7_0_last_secondSecondTen_days_train_5_27_table_click1 h
	   on a.user_id=h.user_id and a.brand_id=h.brand_id
	   left outer join wx1_6_7_0_last_secondThirdTen_days_train_5_27_table_click1 i
	   on a.user_id=i.user_id and a.brand_id=i.brand_id
	   """);
configs = [('last5_clickNum', 'null', '0'),('last21_clickNum', 'null', '0'),
          ('firstOneTen_clickNum', 'null', '0'),('firstSecondTen_clickNum', 'null', '0'),
		  ('firstThirdTen_clickNum', 'null', '0'),('secondOneTen_clickNum', 'null', '0'),
          ('secondSecondTen_clickNum', 'null', '0'),('secondThirdTen_clickNum', 'null', '0')]
DataProc.fillMissingValues('wx1_6_7_0_train_5_19_5_27_table_data123_quan123', 'wx1_6_7_0_train_5_19_5_27_table_data123_quan12345', configs);

#/最后5天及21天点击
sql("""create table wx1_6_7_0_last_five_days_predict_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as last5_clickNum
     from source_data
     where type=0 and visit_datetime>='08-11' and visit_datetime<='08-15'
     group by user_id,brand_id""");

sql("""create table wx1_6_7_0_last_twentyOne_days_predict_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as last21_clickNum
     from source_data
     where type=0 and visit_datetime>='07-26' and visit_datetime<='08-15'
     group by user_id,brand_id""");
	 
sql("""create table wx1_6_7_0_last_firstOneTen_days_predict_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as firstOneTen_clickNum
     from source_data
     where type=0 and visit_datetime>='05-16' and visit_datetime<='05-25'
     group by user_id,brand_id""");
	 
sql("""create table wx1_6_7_0_last_firstSecondTen_days_predict_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as firstSecondTen_clickNum
     from source_data
     where type=0 and visit_datetime>='05-26' and visit_datetime<='06-04'
     group by user_id,brand_id""");
	 
sql("""create table wx1_6_7_0_last_firstThirdTen_days_predict_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as firstThirdTen_clickNum
     from source_data
     where type=0 and visit_datetime>='06-05' and visit_datetime<='06-15'
     group by user_id,brand_id""");
	 
sql("""create table wx1_6_7_0_last_secondOneTen_days_predict_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as secondOneTen_clickNum
     from source_data
     where type=0 and visit_datetime>='06-16' and visit_datetime<='06-25'
     group by user_id,brand_id""");
	 
sql("""create table wx1_6_7_0_last_secondSecondTen_days_predict_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as secondSecondTen_clickNum
     from source_data
     where type=0 and visit_datetime>='06-26' and visit_datetime<='07-05'
     group by user_id,brand_id""");
	 
sql("""create table wx1_6_7_0_last_secondThirdTen_days_predict_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as secondThirdTen_clickNum
     from source_data
     where type=0 and visit_datetime>='07-06' and visit_datetime<='07-15'
     group by user_id,brand_id""");
	 
sql("""create table wx1_6_7_0_predict_5_19_5_27_table_data123_quan123 as 
       select  a.user_id,a.brand_id,a.clickNum,a.buyNum,a.collectNum,a.cartNum,
	           a.clickNum1,a.buyNum1,a.collectNum1,a.cartNum1,
			   a.clickNum2,a.buyNum2,a.collectNum2,a.cartNum2,
			   a.clickNum3,a.buyNum3,a.collectNum3,a.cartNum3,
			   a.last1_clicknum,a.last3_clickNum,a.last7_clickNum,a.last10_clickNum,
			   a.last15_clickNum,a.last1_buyNum,a.last3_buyNum,a.last7_buyNum,a.last10_buyNum,
			   a.last15_buyNum,a.last1_collectNum,a.last3_collectNum,a.last7_collectNum,
			   a.last10_collectNum,a.last15_collectNum,a.last1_cartNum,a.last3_cartNum,
			   a.last7_cartNum,a.last10_cartNum,a.last15_cartNum,
               a.diff_date_click_num,a.diff_date_buy_num,a.diff_date_collect_num,a.diff_date_cart_num,
			   b.last5_clickNum,c.last21_clickNum,d.firstOneTen_clickNum,e.firstSecondTen_clickNum,
			   f.firstThirdTen_clickNum,g.secondOneTen_clickNum,h.secondSecondTen_clickNum,
			   i.secondThirdTen_clickNum
	   from wx1_5_19_3_predict_5_19_5_27_table_data123_quan12345 a
	   left outer join wx1_6_7_0_last_five_days_predict_5_27_table_click1 b
	   on a.user_id=b.user_id and a.brand_id=b.brand_id
	   left outer join wx1_6_7_0_last_twentyOne_days_predict_5_27_table_click1 c
	   on a.user_id=c.user_id and a.brand_id=c.brand_id
	   left outer join wx1_6_7_0_last_firstOneTen_days_predict_5_27_table_click1 d
	   on a.user_id=d.user_id and a.brand_id=d.brand_id
	   left outer join wx1_6_7_0_last_firstSecondTen_days_predict_5_27_table_click1 e
	   on a.user_id=e.user_id and a.brand_id=e.brand_id
	   left outer join wx1_6_7_0_last_firstThirdTen_days_predict_5_27_table_click1 f
	   on a.user_id=f.user_id and a.brand_id=f.brand_id
	   left outer join wx1_6_7_0_last_secondOneTen_days_predict_5_27_table_click1 g
	   on a.user_id=g.user_id and a.brand_id=g.brand_id
	   left outer join wx1_6_7_0_last_secondSecondTen_days_predict_5_27_table_click1 h
	   on a.user_id=h.user_id and a.brand_id=h.brand_id
	   left outer join wx1_6_7_0_last_secondThirdTen_days_predict_5_27_table_click1 i
	   on a.user_id=i.user_id and a.brand_id=i.brand_id
	   """);
configs = [('last5_clickNum', 'null', '0'),('last21_clickNum', 'null', '0'),
          ('firstOneTen_clickNum', 'null', '0'),('firstSecondTen_clickNum', 'null', '0'),
		  ('firstThirdTen_clickNum', 'null', '0'),('secondOneTen_clickNum', 'null', '0'),
          ('secondSecondTen_clickNum', 'null', '0'),('secondThirdTen_clickNum', 'null', '0')]
DataProc.fillMissingValues('wx1_6_7_0_predict_5_19_5_27_table_data123_quan123', 'wx1_6_7_0_predict_5_19_5_27_table_data123_quan12345', configs);


#/**********十天十天算
sql("""create table wx1_6_16_0_last_firstOneTen_days_train_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as firstOneTen_buyNum
     from source_data
     where type=1 and visit_datetime>='04-15' and visit_datetime<='04-24'
     group by user_id,brand_id""");
	 
sql("""create table wx1_6_16_0_last_firstSecondTen_days_train_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as firstSecondTen_buyNum
     from source_data
     where type=1 and visit_datetime>='04-25' and visit_datetime<='05-04'
     group by user_id,brand_id""");
	 
sql("""create table wx1_6_16_0_last_firstThirdTen_days_train_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as firstThirdTen_buyNum
     from source_data
     where type=1 and visit_datetime>='05-05' and visit_datetime<='05-15'
     group by user_id,brand_id""");
	 
sql("""create table wx1_6_16_0_last_secondOneTen_days_train_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as secondOneTen_buyNum
     from source_data
     where type=1 and visit_datetime>='05-16' and visit_datetime<='05-25'
     group by user_id,brand_id""");
	 
sql("""create table wx1_6_16_0_last_secondSecondTen_days_train_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as secondSecondTen_buyNum
     from source_data
     where type=1 and visit_datetime>='05-26' and visit_datetime<='06-04'
     group by user_id,brand_id""");
	 
sql("""create table wx1_6_16_0_last_secondThirdTen_days_train_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as secondThirdTen_buyNum
     from source_data
     where type=1 and visit_datetime>='06-05' and visit_datetime<='06-15'
     group by user_id,brand_id""");
	 
sql("""create table wx1_6_16_0_train_5_19_5_27_table_data123_quan123 as 
       select  a.user_id,a.brand_id,a.clickNum,a.buyNum,a.collectNum,a.cartNum,a.buyYN,
	           a.clickNum1,a.buyNum1,a.collectNum1,a.cartNum1,
			   a.clickNum2,a.buyNum2,a.collectNum2,a.cartNum2,
			   a.clickNum3,a.buyNum3,a.collectNum3,a.cartNum3,
			   a.last1_clicknum,a.last3_clickNum,a.last7_clickNum,a.last10_clickNum,
			   a.last15_clickNum,a.last1_buyNum,a.last3_buyNum,a.last7_buyNum,a.last10_buyNum,
			   a.last15_buyNum,a.last1_collectNum,a.last3_collectNum,a.last7_collectNum,
			   a.last10_collectNum,a.last15_collectNum,a.last1_cartNum,a.last3_cartNum,
			   a.last7_cartNum,a.last10_cartNum,a.last15_cartNum,
               a.diff_date_click_num,a.diff_date_buy_num,a.diff_date_collect_num,a.diff_date_cart_num,
			   a.last5_clickNum,a.last21_clickNum,a.firstOneTen_clickNum,a.firstSecondTen_clickNum,
			   a.firstThirdTen_clickNum,a.secondOneTen_clickNum,a.secondSecondTen_clickNum,a.secondThirdTen_clickNum,
			   b.firstOneTen_buyNum,c.firstSecondTen_buyNum,d.firstThirdTen_buyNum,
			   e.secondOneTen_buyNum,f.secondSecondTen_buyNum,g.secondThirdTen_buyNum
	   from wx1_6_7_0_train_5_19_5_27_table_data123_quan12345 a
	   left outer join wx1_6_16_0_last_firstOneTen_days_train_5_27_table_click1 b
	   on a.user_id=b.user_id and a.brand_id=b.brand_id
	   left outer join wx1_6_16_0_last_firstSecondTen_days_train_5_27_table_click1 c
	   on a.user_id=c.user_id and a.brand_id=c.brand_id
	   left outer join wx1_6_16_0_last_firstThirdTen_days_train_5_27_table_click1 d
	   on a.user_id=d.user_id and a.brand_id=d.brand_id
	   left outer join wx1_6_16_0_last_secondOneTen_days_train_5_27_table_click1 e
	   on a.user_id=e.user_id and a.brand_id=e.brand_id
	   left outer join wx1_6_16_0_last_secondSecondTen_days_train_5_27_table_click1 f
	   on a.user_id=f.user_id and a.brand_id=f.brand_id
	   left outer join wx1_6_16_0_last_secondThirdTen_days_train_5_27_table_click1 g
	   on a.user_id=g.user_id and a.brand_id=g.brand_id
	   """);
configs = [('firstOneTen_buyNum', 'null', '0'),('firstSecondTen_buyNum', 'null', '0'),
		  ('firstThirdTen_buyNum', 'null', '0'),('secondOneTen_buyNum', 'null', '0'),
          ('secondSecondTen_buyNum', 'null', '0'),('secondThirdTen_buyNum', 'null', '0')]
DataProc.fillMissingValues('wx1_6_16_0_train_5_19_5_27_table_data123_quan123', 'wx1_6_16_0_train_5_19_5_27_table_data123_quan12345', configs);

#/十天十天算
sql("""create table wx1_6_16_0_last_firstOneTen_days_predict_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as firstOneTen_buyNum
     from source_data
     where type=1 and visit_datetime>='05-16' and visit_datetime<='05-25'
     group by user_id,brand_id""");
	 
sql("""create table wx1_6_16_0_last_firstSecondTen_days_predict_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as firstSecondTen_buyNum
     from source_data
     where type=1 and visit_datetime>='05-26' and visit_datetime<='06-04'
     group by user_id,brand_id""");
	 
sql("""create table wx1_6_16_0_last_firstThirdTen_days_predict_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as firstThirdTen_buyNum
     from source_data
     where type=1 and visit_datetime>='06-05' and visit_datetime<='06-15'
     group by user_id,brand_id""");
	 
sql("""create table wx1_6_16_0_last_secondOneTen_days_predict_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as secondOneTen_buyNum
     from source_data
     where type=1 and visit_datetime>='06-16' and visit_datetime<='06-25'
     group by user_id,brand_id""");
	 
sql("""create table wx1_6_16_0_last_secondSecondTen_days_predict_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as secondSecondTen_buyNum
     from source_data
     where type=1 and visit_datetime>='06-26' and visit_datetime<='07-05'
     group by user_id,brand_id""");
	 
sql("""create table wx1_6_16_0_last_secondThirdTen_days_predict_5_27_table_click1 as
     select user_id,brand_id,count(brand_id) as secondThirdTen_buyNum
     from source_data
     where type=1 and visit_datetime>='07-06' and visit_datetime<='07-15'
     group by user_id,brand_id""");
	 
sql("""create table wx1_6_16_0_predict_5_19_5_27_table_data123_quan123 as 
       select  a.user_id,a.brand_id,a.clickNum,a.buyNum,a.collectNum,a.cartNum,
	           a.clickNum1,a.buyNum1,a.collectNum1,a.cartNum1,
			   a.clickNum2,a.buyNum2,a.collectNum2,a.cartNum2,
			   a.clickNum3,a.buyNum3,a.collectNum3,a.cartNum3,
			   a.last1_clicknum,a.last3_clickNum,a.last7_clickNum,a.last10_clickNum,
			   a.last15_clickNum,a.last1_buyNum,a.last3_buyNum,a.last7_buyNum,a.last10_buyNum,
			   a.last15_buyNum,a.last1_collectNum,a.last3_collectNum,a.last7_collectNum,
			   a.last10_collectNum,a.last15_collectNum,a.last1_cartNum,a.last3_cartNum,
			   a.last7_cartNum,a.last10_cartNum,a.last15_cartNum,
               a.diff_date_click_num,a.diff_date_buy_num,a.diff_date_collect_num,a.diff_date_cart_num,
			   a.last5_clickNum,a.last21_clickNum,a.firstOneTen_clickNum,a.firstSecondTen_clickNum,
			   a.firstThirdTen_clickNum,a.secondOneTen_clickNum,a.secondSecondTen_clickNum,a.secondThirdTen_clickNum,
			   b.firstOneTen_buyNum,c.firstSecondTen_buyNum,d.firstThirdTen_buyNum,
			   e.secondOneTen_buyNum,f.secondSecondTen_buyNum,g.secondThirdTen_buyNum
	   from wx1_6_7_0_predict_5_19_5_27_table_data123_quan12345 a
	   left outer join wx1_6_16_0_last_firstOneTen_days_predict_5_27_table_click1 b
	   on a.user_id=b.user_id and a.brand_id=b.brand_id
	   left outer join wx1_6_16_0_last_firstSecondTen_days_predict_5_27_table_click1 c
	   on a.user_id=c.user_id and a.brand_id=c.brand_id
	   left outer join wx1_6_16_0_last_firstThirdTen_days_predict_5_27_table_click1 d
	   on a.user_id=d.user_id and a.brand_id=d.brand_id
	   left outer join wx1_6_16_0_last_secondOneTen_days_predict_5_27_table_click1 e
	   on a.user_id=e.user_id and a.brand_id=e.brand_id
	   left outer join wx1_6_16_0_last_secondSecondTen_days_predict_5_27_table_click1 f
	   on a.user_id=f.user_id and a.brand_id=f.brand_id
	   left outer join wx1_6_16_0_last_secondThirdTen_days_predict_5_27_table_click1 g
	   on a.user_id=g.user_id and a.brand_id=g.brand_id
	   """);
configs = [('firstOneTen_buyNum', 'null', '0'),('firstSecondTen_buyNum', 'null', '0'),
		  ('firstThirdTen_buyNum', 'null', '0'),('secondOneTen_buyNum', 'null', '0'),
          ('secondSecondTen_buyNum', 'null', '0'),('secondThirdTen_buyNum', 'null', '0')]
DataProc.fillMissingValues('wx1_6_16_0_predict_5_19_5_27_table_data123_quan123', 'wx1_6_16_0_predict_5_19_5_27_table_data123_quan12345', configs);


#/点击但没购买的
sql("""create table wx1_6_15_6_example_positive_negative_train_5_27_table as select
user_id,brand_id,last10_clickNum
from wx1_6_16_0_train_5_19_5_27_table_data123_quan12345 where last10_buyNum=0""");

sql("""alter table wx1_6_15_6_example_positive_negative_train_5_27_table change column last10_clickNum
rename to last10_noBuy_click""");

sql("""create table wx1_6_15_7_example_positive_negative_train_5_27_table as select
a.user_id,a.brand_id,a.buyYN,
	           a.clickNum1,a.buyNum1,a.collectNum1,a.cartNum1,
			   a.clickNum2,a.buyNum2,a.collectNum2,a.cartNum2,
			   a.clickNum3,a.buyNum3,a.collectNum3,a.cartNum3,
			   a.last1_clicknum,a.last3_clickNum,a.last7_clickNum,a.last10_clickNum,
			   a.last15_clickNum,a.last1_buyNum,a.last3_buyNum,a.last7_buyNum,a.last10_buyNum,
			   a.last15_buyNum,a.last1_collectNum,a.last3_collectNum,a.last7_collectNum,
			   a.last10_collectNum,a.last15_collectNum,a.last1_cartNum,a.last3_cartNum,
			   a.last7_cartNum,a.last10_cartNum,a.last15_cartNum,
               a.diff_date_click_num,a.diff_date_buy_num,a.diff_date_collect_num,a.diff_date_cart_num,
			   a.last5_clickNum,a.last21_clickNum,a.firstOneTen_clickNum,
			   a.firstSecondTen_clickNum,
			   a.firstThirdTen_clickNum,a.secondOneTen_clickNum,a.secondSecondTen_clickNum,
			   a.secondThirdTen_clickNum,
			   a.firstOneTen_buyNum,a.firstSecondTen_buyNum,a.firstThirdTen_buyNum,
			   a.secondOneTen_buyNum,a.secondSecondTen_buyNum,a.secondThirdTen_buyNum,
			   b.last10_noBuy_click 
from wx1_6_16_0_train_5_19_5_27_table_data123_quan12345 a
left outer join wx1_6_15_6_example_positive_negative_train_5_27_table b
on a.user_id=b.user_id and a.brand_id=b.brand_id""");

configs = [('last10_noBuy_click', 'null', '0')]
DataProc.fillMissingValues('wx1_6_15_7_example_positive_negative_train_5_27_table', 
'wx1_6_15_7_example_positive_negative_train_5_27_table123', configs);

sql("""create table wx1_6_15_6_predict_5_19_5_27_table_data123_quan12345 as select
user_id,brand_id,last10_clickNum
from wx1_6_16_0_predict_5_19_5_27_table_data123_quan12345 where last10_buyNum=0""");

sql("""alter table wx1_6_15_6_predict_5_19_5_27_table_data123_quan12345 change column last10_clickNum
rename to last10_noBuy_click""");

sql("""create table wx1_6_15_7_example_positive_negative_predict_5_27_table as select
a.user_id,a.brand_id,
	           a.clickNum1,a.buyNum1,a.collectNum1,a.cartNum1,
			   a.clickNum2,a.buyNum2,a.collectNum2,a.cartNum2,
			   a.clickNum3,a.buyNum3,a.collectNum3,a.cartNum3,
			   a.last1_clicknum,a.last3_clickNum,a.last7_clickNum,a.last10_clickNum,
			   a.last15_clickNum,a.last1_buyNum,a.last3_buyNum,a.last7_buyNum,a.last10_buyNum,
			   a.last15_buyNum,a.last1_collectNum,a.last3_collectNum,a.last7_collectNum,
			   a.last10_collectNum,a.last15_collectNum,a.last1_cartNum,a.last3_cartNum,
			   a.last7_cartNum,a.last10_cartNum,a.last15_cartNum,
               a.diff_date_click_num,a.diff_date_buy_num,a.diff_date_collect_num,a.diff_date_cart_num,
			   a.last5_clickNum,a.last21_clickNum,a.firstOneTen_clickNum,
			   a.firstSecondTen_clickNum,
			   a.firstThirdTen_clickNum,a.secondOneTen_clickNum,a.secondSecondTen_clickNum,
			   a.secondThirdTen_clickNum,
			   a.firstOneTen_buyNum,a.firstSecondTen_buyNum,a.firstThirdTen_buyNum,
			   a.secondOneTen_buyNum,a.secondSecondTen_buyNum,a.secondThirdTen_buyNum,
			   b.last10_noBuy_click
from wx1_6_16_0_predict_5_19_5_27_table_data123_quan12345 a
left outer join wx1_6_15_6_predict_5_19_5_27_table_data123_quan12345 b
on a.user_id=b.user_id and a.brand_id=b.brand_id""");

configs = [('last10_noBuy_click', 'null', '0')]
DataProc.fillMissingValues('wx1_6_15_7_example_positive_negative_predict_5_27_table', 'wx1_6_15_7_example_positive_negative_predict_5_27_table123', configs);

sql("""create table wx1_6_15_30_5_example_positive_negative_predict_5_27_table as select
user_id,brand_id,last1_clickNum
from wx1_6_15_7_example_positive_negative_predict_5_27_table where last1_buyNum=0""");

sql("""alter table wx1_6_15_30_5_example_positive_negative_predict_5_27_table change column last1_clickNum
rename to last1_noBuy_click""");

sql("""create table wx1_6_15_30_6_example_positive_negative_predict_5_27_table as select
user_id,brand_id,last3_clickNum
from wx1_6_15_7_example_positive_negative_predict_5_27_table where last3_buyNum=0""");

sql("""alter table wx1_6_15_30_6_example_positive_negative_predict_5_27_table change column last3_clickNum
rename to last3_noBuy_click""");

sql("""create table wx1_6_15_30_7_example_positive_negative_predict_5_27_table as select
user_id,brand_id,last7_clickNum
from wx1_6_15_7_example_positive_negative_predict_5_27_table where last7_buyNum=0""");

sql("""alter table wx1_6_15_30_7_example_positive_negative_predict_5_27_table change column last7_clickNum
rename to last7_noBuy_click""");

sql("""create table wx1_6_15_30_8_example_positive_negative_predict_5_27_table as select
user_id,brand_id,last15_clickNum
from wx1_6_15_7_example_positive_negative_predict_5_27_table where last15_buyNum=0""");

sql("""alter table wx1_6_15_30_8_example_positive_negative_predict_5_27_table change column last15_clickNum
rename to last15_noBuy_click""");

sql("""create table wx1_6_15_31_5_example_positive_negative_predict_5_27_table as select
a.user_id,a.brand_id,
	           a.clickNum1,a.buyNum1,a.collectNum1,a.cartNum1,
			   a.clickNum2,a.buyNum2,a.collectNum2,a.cartNum2,
			   a.clickNum3,a.buyNum3,a.collectNum3,a.cartNum3,
			   a.last1_clicknum,a.last3_clickNum,a.last7_clickNum,a.last10_clickNum,
			   a.last15_clickNum,a.last1_buyNum,a.last3_buyNum,a.last7_buyNum,a.last10_buyNum,
			   a.last15_buyNum,a.last1_collectNum,a.last3_collectNum,a.last7_collectNum,
			   a.last10_collectNum,a.last15_collectNum,a.last1_cartNum,a.last3_cartNum,
			   a.last7_cartNum,a.last10_cartNum,a.last15_cartNum,
               a.diff_date_click_num,a.diff_date_buy_num,a.diff_date_collect_num,a.diff_date_cart_num,
			   a.last10_noBuy_click,
			   a.last5_clickNum,a.last21_clickNum,a.firstOneTen_clickNum,
			   a.firstSecondTen_clickNum,
			   a.firstThirdTen_clickNum,a.secondOneTen_clickNum,a.secondSecondTen_clickNum,
			   a.secondThirdTen_clickNum,
			   a.firstOneTen_buyNum,a.firstSecondTen_buyNum,a.firstThirdTen_buyNum,
			   a.secondOneTen_buyNum,a.secondSecondTen_buyNum,a.secondThirdTen_buyNum,
			   b.last1_noBuy_click,c.last3_noBuy_click,d.last7_noBuy_click,
			   e.last15_noBuy_click
from wx1_6_15_7_example_positive_negative_predict_5_27_table123 a
left outer join wx1_6_15_30_5_example_positive_negative_predict_5_27_table b
on a.user_id=b.user_id and a.brand_id=b.brand_id
left outer join wx1_6_15_30_6_example_positive_negative_predict_5_27_table c
on a.user_id=c.user_id and a.brand_id=c.brand_id
left outer join wx1_6_15_30_7_example_positive_negative_predict_5_27_table d
on a.user_id=d.user_id and a.brand_id=d.brand_id
left outer join wx1_6_15_30_8_example_positive_negative_predict_5_27_table e
on a.user_id=e.user_id and a.brand_id=e.brand_id""");

configs = [('last1_noBuy_click', 'null', '0'),('last3_noBuy_click', 'null', '0'),
('last7_noBuy_click', 'null', '0'),('last15_noBuy_click', 'null', '0')]
DataProc.fillMissingValues('wx1_6_15_31_5_example_positive_negative_predict_5_27_table', 'wx1_6_15_31_5_example_positive_negative_predict_5_27_table123', configs);

sql("""create table wx1_6_15_30_5_example_positive_negative_train_5_27_table as select
user_id,brand_id,last1_clickNum
from wx1_6_15_7_example_positive_negative_train_5_27_table where last1_buyNum=0""");

sql("""alter table wx1_6_15_30_5_example_positive_negative_train_5_27_table change column last1_clickNum
rename to last1_noBuy_click""");

sql("""create table wx1_6_15_30_6_example_positive_negative_train_5_27_table as select
user_id,brand_id,last3_clickNum
from wx1_6_15_7_example_positive_negative_train_5_27_table where last3_buyNum=0""");

sql("""alter table wx1_6_15_30_6_example_positive_negative_train_5_27_table change column last3_clickNum
rename to last3_noBuy_click""");

sql("""create table wx1_6_15_30_7_example_positive_negative_train_5_27_table as select
user_id,brand_id,last7_clickNum
from wx1_6_15_7_example_positive_negative_train_5_27_table where last7_buyNum=0""");

sql("""alter table wx1_6_15_30_7_example_positive_negative_train_5_27_table change column last7_clickNum
rename to last7_noBuy_click""");

sql("""create table wx1_6_15_30_8_example_positive_negative_train_5_27_table as select
user_id,brand_id,last15_clickNum
from wx1_6_15_7_example_positive_negative_train_5_27_table where last15_buyNum=0""");

sql("""alter table wx1_6_15_30_8_example_positive_negative_train_5_27_table change column last15_clickNum
rename to last15_noBuy_click""");

sql("""create table wx1_6_15_31_5_example_positive_negative_train_5_27_table as select
a.user_id,a.brand_id,a.buyYN,
	           a.clickNum1,a.buyNum1,a.collectNum1,a.cartNum1,
			   a.clickNum2,a.buyNum2,a.collectNum2,a.cartNum2,
			   a.clickNum3,a.buyNum3,a.collectNum3,a.cartNum3,
			   a.last1_clicknum,a.last3_clickNum,a.last7_clickNum,a.last10_clickNum,
			   a.last15_clickNum,a.last1_buyNum,a.last3_buyNum,a.last7_buyNum,a.last10_buyNum,
			   a.last15_buyNum,a.last1_collectNum,a.last3_collectNum,a.last7_collectNum,
			   a.last10_collectNum,a.last15_collectNum,a.last1_cartNum,a.last3_cartNum,
			   a.last7_cartNum,a.last10_cartNum,a.last15_cartNum,
               a.diff_date_click_num,a.diff_date_buy_num,a.diff_date_collect_num,a.diff_date_cart_num,
			   a.last10_noBuy_click,
			   a.last5_clickNum,a.last21_clickNum,a.firstOneTen_clickNum,
			   a.firstSecondTen_clickNum,
			   a.firstThirdTen_clickNum,a.secondOneTen_clickNum,a.secondSecondTen_clickNum,
			   a.secondThirdTen_clickNum,
			   a.firstOneTen_buyNum,a.firstSecondTen_buyNum,a.firstThirdTen_buyNum,
			   a.secondOneTen_buyNum,a.secondSecondTen_buyNum,a.secondThirdTen_buyNum,
			   b.last1_noBuy_click,c.last3_noBuy_click,d.last7_noBuy_click,
			   e.last15_noBuy_click
from wx1_6_15_7_example_positive_negative_train_5_27_table123 a
left outer join wx1_6_15_30_5_example_positive_negative_train_5_27_table b
on a.user_id=b.user_id and a.brand_id=b.brand_id
left outer join wx1_6_15_30_6_example_positive_negative_train_5_27_table c
on a.user_id=c.user_id and a.brand_id=c.brand_id
left outer join wx1_6_15_30_7_example_positive_negative_train_5_27_table d
on a.user_id=d.user_id and a.brand_id=d.brand_id
left outer join wx1_6_15_30_8_example_positive_negative_train_5_27_table e
on a.user_id=e.user_id and a.brand_id=e.brand_id""");

configs = [('last1_noBuy_click', 'null', '0'),('last3_noBuy_click', 'null', '0'),
('last7_noBuy_click', 'null', '0'),('last15_noBuy_click', 'null', '0')]
DataProc.fillMissingValues('wx1_6_15_31_5_example_positive_negative_train_5_27_table', 
'wx1_6_15_31_5_example_positive_negative_train_5_27_table123', configs);


sql("""create table wx1_6_151_6_example_positive_negative_train_5_27_table as select
user_id,brand_id,clickNum1
from wx1_6_15_7_example_positive_negative_train_5_27_table where buyNum1=0""");

sql("""alter table wx1_6_151_6_example_positive_negative_train_5_27_table change column clickNum1
rename to click1_noBuy_click""");

sql("""create table wx1_6_151_7_example_positive_negative_train_5_27_table as select
user_id,brand_id,clickNum2
from wx1_6_15_7_example_positive_negative_train_5_27_table where buyNum2=0""");

sql("""alter table wx1_6_151_7_example_positive_negative_train_5_27_table change column clickNum2
rename to click2_noBuy_click""");

sql("""create table wx1_6_151_8_example_positive_negative_train_5_27_table as select
user_id,brand_id,clickNum3
from wx1_6_15_7_example_positive_negative_train_5_27_table where buyNum3=0""");

sql("""alter table wx1_6_151_8_example_positive_negative_train_5_27_table change column clickNum3
rename to click3_noBuy_click""");

sql("""create table wx1_6_152_5_example_positive_negative_train_5_27_table as select
a.user_id,a.brand_id,a.buyYN,
	           a.clickNum1,a.buyNum1,a.collectNum1,a.cartNum1,
			   a.clickNum2,a.buyNum2,a.collectNum2,a.cartNum2,
			   a.clickNum3,a.buyNum3,a.collectNum3,a.cartNum3,
			   a.last1_clicknum,a.last3_clickNum,a.last7_clickNum,a.last10_clickNum,
			   a.last15_clickNum,a.last1_buyNum,a.last3_buyNum,a.last7_buyNum,a.last10_buyNum,
			   a.last15_buyNum,a.last1_collectNum,a.last3_collectNum,a.last7_collectNum,
			   a.last10_collectNum,a.last15_collectNum,a.last1_cartNum,a.last3_cartNum,
			   a.last7_cartNum,a.last10_cartNum,a.last15_cartNum,
               a.diff_date_click_num,a.diff_date_buy_num,a.diff_date_collect_num,a.diff_date_cart_num,
			   a.last10_noBuy_click,a.last1_noBuy_click,a.last3_noBuy_click,a.last7_noBuy_click,
			   a.last15_noBuy_click,
			   a.last5_clickNum,a.last21_clickNum,a.firstOneTen_clickNum,
			   a.firstSecondTen_clickNum,
			   a.firstThirdTen_clickNum,a.secondOneTen_clickNum,a.secondSecondTen_clickNum,
			   a.secondThirdTen_clickNum,
			   a.firstOneTen_buyNum,a.firstSecondTen_buyNum,a.firstThirdTen_buyNum,
			   a.secondOneTen_buyNum,a.secondSecondTen_buyNum,a.secondThirdTen_buyNum,
			   b.click1_noBuy_click,c.click2_noBuy_click,d.click3_noBuy_click
from wx1_6_15_31_5_example_positive_negative_train_5_27_table123 a
left outer join wx1_6_151_6_example_positive_negative_train_5_27_table b
on a.user_id=b.user_id and a.brand_id=b.brand_id
left outer join wx1_6_151_7_example_positive_negative_train_5_27_table c
on a.user_id=c.user_id and a.brand_id=c.brand_id
left outer join wx1_6_151_8_example_positive_negative_train_5_27_table d
on a.user_id=d.user_id and a.brand_id=d.brand_id""");

configs = [('click1_noBuy_click', 'null', '0'),('click2_noBuy_click', 'null', '0'),
('click3_noBuy_click', 'null', '0')]
DataProc.fillMissingValues('wx1_6_152_5_example_positive_negative_train_5_27_table', 
'wx1_6_152_5_example_positive_negative_train_5_27_table123', configs);


sql("""create table wx1_6_151_6_example_positive_negative_predict_5_27_table as select
user_id,brand_id,clickNum1
from wx1_6_15_7_example_positive_negative_predict_5_27_table where buyNum1=0""");

sql("""alter table wx1_6_151_6_example_positive_negative_predict_5_27_table change column clickNum1
rename to click1_noBuy_click""");

sql("""create table wx1_6_151_7_example_positive_negative_predict_5_27_table as select
user_id,brand_id,clickNum2
from wx1_6_15_7_example_positive_negative_predict_5_27_table where buyNum2=0""");

sql("""alter table wx1_6_151_7_example_positive_negative_predict_5_27_table change column clickNum2
rename to click2_noBuy_click""");

sql("""create table wx1_6_151_8_example_positive_negative_predict_5_27_table as select
user_id,brand_id,clickNum3
from wx1_6_15_7_example_positive_negative_predict_5_27_table where buyNum3=0""");

sql("""alter table wx1_6_151_8_example_positive_negative_predict_5_27_table change column clickNum3
rename to click3_noBuy_click""");

sql("""create table wx1_6_152_5_example_positive_negative_predict_5_27_table as select
a.user_id,a.brand_id,
	           a.clickNum1,a.buyNum1,a.collectNum1,a.cartNum1,
			   a.clickNum2,a.buyNum2,a.collectNum2,a.cartNum2,
			   a.clickNum3,a.buyNum3,a.collectNum3,a.cartNum3,
			   a.last1_clicknum,a.last3_clickNum,a.last7_clickNum,a.last10_clickNum,
			   a.last15_clickNum,a.last1_buyNum,a.last3_buyNum,a.last7_buyNum,a.last10_buyNum,
			   a.last15_buyNum,a.last1_collectNum,a.last3_collectNum,a.last7_collectNum,
			   a.last10_collectNum,a.last15_collectNum,a.last1_cartNum,a.last3_cartNum,
			   a.last7_cartNum,a.last10_cartNum,a.last15_cartNum,
               a.diff_date_click_num,a.diff_date_buy_num,a.diff_date_collect_num,a.diff_date_cart_num,
			   a.last10_noBuy_click,a.last1_noBuy_click,a.last3_noBuy_click,a.last7_noBuy_click,
			   a.last15_noBuy_click,
			   a.last5_clickNum,a.last21_clickNum,a.firstOneTen_clickNum,
			   a.firstSecondTen_clickNum,
			   a.firstThirdTen_clickNum,a.secondOneTen_clickNum,a.secondSecondTen_clickNum,
			   a.secondThirdTen_clickNum,
			   a.firstOneTen_buyNum,a.firstSecondTen_buyNum,a.firstThirdTen_buyNum,
			   a.secondOneTen_buyNum,a.secondSecondTen_buyNum,a.secondThirdTen_buyNum,
			   b.click1_noBuy_click,c.click2_noBuy_click,d.click3_noBuy_click
from wx1_6_15_31_5_example_positive_negative_predict_5_27_table123 a
left outer join wx1_6_151_6_example_positive_negative_predict_5_27_table b
on a.user_id=b.user_id and a.brand_id=b.brand_id
left outer join wx1_6_151_7_example_positive_negative_predict_5_27_table c
on a.user_id=c.user_id and a.brand_id=c.brand_id
left outer join wx1_6_151_8_example_positive_negative_predict_5_27_table d
on a.user_id=d.user_id and a.brand_id=d.brand_id""");

configs = [('click1_noBuy_click', 'null', '0'),('click2_noBuy_click', 'null', '0'),
('click3_noBuy_click', 'null', '0')]
DataProc.fillMissingValues('wx1_6_152_5_example_positive_negative_predict_5_27_table', 'wx1_6_152_5_example_positive_negative_predict_5_27_table123', configs);



sql("""create table wx1_6_75_6_example_positive_negative_train_5_27_table as select
user_id,brand_id,firstOneTen_clickNum
from wx1_6_16_0_train_5_19_5_27_table_data123_quan12345 where firstOneTen_buyNum=0""");

sql("""alter table wx1_6_75_6_example_positive_negative_train_5_27_table change column firstOneTen_clickNum
rename to firstOneTen_noBuy_click""");

sql("""create table wx1_6_75_7_example_positive_negative_train_5_27_table as select
user_id,brand_id,firstSecondTen_clickNum
from wx1_6_16_0_train_5_19_5_27_table_data123_quan12345 where firstSecondTen_buyNum=0""");

sql("""alter table wx1_6_75_7_example_positive_negative_train_5_27_table change column firstSecondTen_clickNum
rename to firstSecondTen_noBuy_click""");

sql("""create table wx1_6_75_8_example_positive_negative_train_5_27_table as select
user_id,brand_id,firstThirdTen_clickNum
from wx1_6_16_0_train_5_19_5_27_table_data123_quan12345 where firstThirdTen_buyNum=0""");

sql("""alter table wx1_6_75_8_example_positive_negative_train_5_27_table change column firstThirdTen_clickNum
rename to firstThirdTen_noBuy_click""");

sql("""create table wx1_6_75_9_example_positive_negative_train_5_27_table as select
user_id,brand_id,secondOneTen_clickNum
from wx1_6_16_0_train_5_19_5_27_table_data123_quan12345 where secondOneTen_buyNum=0""");

sql("""alter table wx1_6_75_9_example_positive_negative_train_5_27_table change column secondOneTen_clickNum
rename to secondOneTen_noBuy_click""");

sql("""create table wx1_6_75_10_example_positive_negative_train_5_27_table as select
user_id,brand_id,secondSecondTen_clickNum
from wx1_6_16_0_train_5_19_5_27_table_data123_quan12345 where secondSecondTen_buyNum=0""");

sql("""alter table wx1_6_75_10_example_positive_negative_train_5_27_table change column secondSecondTen_clickNum
rename to secondSecondTen_noBuy_click""");

sql("""create table wx1_6_75_11_example_positive_negative_train_5_27_table as select
user_id,brand_id,secondThirdTen_clickNum
from wx1_6_16_0_train_5_19_5_27_table_data123_quan12345 where secondThirdTen_buyNum=0""");

sql("""alter table wx1_6_75_11_example_positive_negative_train_5_27_table change column secondThirdTen_clickNum
rename to secondThirdTen_noBuy_click""");

sql("""create table wx1_6_76_5_example_positive_negative_train_5_27_table as select
a.user_id,a.brand_id,a.buyYN,
	           a.clickNum1,a.buyNum1,a.collectNum1,a.cartNum1,
			   a.clickNum2,a.buyNum2,a.collectNum2,a.cartNum2,
			   a.clickNum3,a.buyNum3,a.collectNum3,a.cartNum3,
			   a.last1_clicknum,a.last3_clickNum,a.last7_clickNum,a.last10_clickNum,
			   a.last15_clickNum,a.last1_buyNum,a.last3_buyNum,a.last7_buyNum,a.last10_buyNum,
			   a.last15_buyNum,a.last1_collectNum,a.last3_collectNum,a.last7_collectNum,
			   a.last10_collectNum,a.last15_collectNum,a.last1_cartNum,a.last3_cartNum,
			   a.last7_cartNum,a.last10_cartNum,a.last15_cartNum,
               a.diff_date_click_num,a.diff_date_buy_num,a.diff_date_collect_num,a.diff_date_cart_num,
			   a.last10_noBuy_click,a.last1_noBuy_click,a.last3_noBuy_click,a.last7_noBuy_click,
			   a.last15_noBuy_click,
			   a.last5_clickNum,a.last21_clickNum,a.firstOneTen_clickNum,
			   a.firstSecondTen_clickNum,
			   a.firstThirdTen_clickNum,a.secondOneTen_clickNum,a.secondSecondTen_clickNum,
			   a.secondThirdTen_clickNum,a.click1_noBuy_click,a.click2_noBuy_click,a.click3_noBuy_click,
			   a.firstOneTen_buyNum,a.firstSecondTen_buyNum,a.firstThirdTen_buyNum,
			   a.secondOneTen_buyNum,a.secondSecondTen_buyNum,a.secondThirdTen_buyNum,
			   b.firstOneTen_noBuy_click,c.firstSecondTen_noBuy_click,d.firstThirdTen_noBuy_click,
			   e.secondOneTen_noBuy_click,f.secondSecondTen_noBuy_click,g.secondThirdTen_noBuy_click
from wx1_6_152_5_example_positive_negative_train_5_27_table123 a
left outer join wx1_6_75_6_example_positive_negative_train_5_27_table b
on a.user_id=b.user_id and a.brand_id=b.brand_id
left outer join wx1_6_75_7_example_positive_negative_train_5_27_table c
on a.user_id=c.user_id and a.brand_id=c.brand_id
left outer join wx1_6_75_8_example_positive_negative_train_5_27_table d
on a.user_id=d.user_id and a.brand_id=d.brand_id
left outer join wx1_6_75_9_example_positive_negative_train_5_27_table e
on a.user_id=e.user_id and a.brand_id=e.brand_id
left outer join wx1_6_75_10_example_positive_negative_train_5_27_table f
on a.user_id=f.user_id and a.brand_id=f.brand_id
left outer join wx1_6_75_11_example_positive_negative_train_5_27_table g
on a.user_id=g.user_id and a.brand_id=g.brand_id""");

configs = [('firstOneTen_noBuy_click', 'null', '0'),('firstSecondTen_noBuy_click', 'null', '0'),
('firstThirdTen_noBuy_click', 'null', '0'),
('secondOneTen_noBuy_click', 'null', '0'),('secondSecondTen_noBuy_click', 'null', '0'),
('secondThirdTen_noBuy_click', 'null', '0')]
DataProc.fillMissingValues('wx1_6_76_5_example_positive_negative_train_5_27_table', 
'wx1_6_76_5_example_positive_negative_train_5_27_table123', configs);


sql("""create table wx1_6_75_6_example_positive_negative_predict_5_27_table as select
user_id,brand_id,firstOneTen_clickNum
from wx1_6_16_0_predict_5_19_5_27_table_data123_quan12345 where firstOneTen_buyNum=0""");

sql("""alter table wx1_6_75_6_example_positive_negative_predict_5_27_table change column firstOneTen_clickNum
rename to firstOneTen_noBuy_click""");

sql("""create table wx1_6_75_7_example_positive_negative_predict_5_27_table as select
user_id,brand_id,firstSecondTen_clickNum
from wx1_6_16_0_predict_5_19_5_27_table_data123_quan12345 where firstSecondTen_buyNum=0""");

sql("""alter table wx1_6_75_7_example_positive_negative_predict_5_27_table change column firstSecondTen_clickNum
rename to firstSecondTen_noBuy_click""");

sql("""create table wx1_6_75_8_example_positive_negative_predict_5_27_table as select
user_id,brand_id,firstThirdTen_clickNum
from wx1_6_16_0_predict_5_19_5_27_table_data123_quan12345 where firstThirdTen_buyNum=0""");

sql("""alter table wx1_6_75_8_example_positive_negative_predict_5_27_table change column firstThirdTen_clickNum
rename to firstThirdTen_noBuy_click""");

sql("""create table wx1_6_75_9_example_positive_negative_predict_5_27_table as select
user_id,brand_id,secondOneTen_clickNum
from wx1_6_16_0_predict_5_19_5_27_table_data123_quan12345 where secondOneTen_buyNum=0""");

sql("""alter table wx1_6_75_9_example_positive_negative_predict_5_27_table change column secondOneTen_clickNum
rename to secondOneTen_noBuy_click""");

sql("""create table wx1_6_75_10_example_positive_negative_predict_5_27_table as select
user_id,brand_id,secondSecondTen_clickNum
from wx1_6_16_0_predict_5_19_5_27_table_data123_quan12345 where secondSecondTen_buyNum=0""");

sql("""alter table wx1_6_75_10_example_positive_negative_predict_5_27_table change column secondSecondTen_clickNum
rename to secondSecondTen_noBuy_click""");

sql("""create table wx1_6_75_11_example_positive_negative_predict_5_27_table as select
user_id,brand_id,secondThirdTen_clickNum
from wx1_6_16_0_predict_5_19_5_27_table_data123_quan12345 where secondThirdTen_buyNum=0""");

sql("""alter table wx1_6_75_11_example_positive_negative_predict_5_27_table change column secondThirdTen_clickNum
rename to secondThirdTen_noBuy_click""");

sql("""create table wx1_6_76_5_example_positive_negative_predict_5_27_table as select
a.user_id,a.brand_id,
	           a.clickNum1,a.buyNum1,a.collectNum1,a.cartNum1,
			   a.clickNum2,a.buyNum2,a.collectNum2,a.cartNum2,
			   a.clickNum3,a.buyNum3,a.collectNum3,a.cartNum3,
			   a.last1_clicknum,a.last3_clickNum,a.last7_clickNum,a.last10_clickNum,
			   a.last15_clickNum,a.last1_buyNum,a.last3_buyNum,a.last7_buyNum,a.last10_buyNum,
			   a.last15_buyNum,a.last1_collectNum,a.last3_collectNum,a.last7_collectNum,
			   a.last10_collectNum,a.last15_collectNum,a.last1_cartNum,a.last3_cartNum,
			   a.last7_cartNum,a.last10_cartNum,a.last15_cartNum,
               a.diff_date_click_num,a.diff_date_buy_num,a.diff_date_collect_num,a.diff_date_cart_num,
			   a.last10_noBuy_click,a.last1_noBuy_click,a.last3_noBuy_click,a.last7_noBuy_click,
			   a.last15_noBuy_click,
			   a.last5_clickNum,a.last21_clickNum,a.firstOneTen_clickNum,
			   a.firstSecondTen_clickNum,
			   a.firstThirdTen_clickNum,a.secondOneTen_clickNum,a.secondSecondTen_clickNum,
			   a.secondThirdTen_clickNum,a.click1_noBuy_click,a.click2_noBuy_click,a.click3_noBuy_click,
			   a.firstOneTen_buyNum,a.firstSecondTen_buyNum,a.firstThirdTen_buyNum,
			   a.secondOneTen_buyNum,a.secondSecondTen_buyNum,a.secondThirdTen_buyNum,
			   b.firstOneTen_noBuy_click,c.firstSecondTen_noBuy_click,d.firstThirdTen_noBuy_click,
			   e.secondOneTen_noBuy_click,f.secondSecondTen_noBuy_click,g.secondThirdTen_noBuy_click
from wx1_6_152_5_example_positive_negative_predict_5_27_table123 a
left outer join wx1_6_75_6_example_positive_negative_predict_5_27_table b
on a.user_id=b.user_id and a.brand_id=b.brand_id
left outer join wx1_6_75_7_example_positive_negative_predict_5_27_table c
on a.user_id=c.user_id and a.brand_id=c.brand_id
left outer join wx1_6_75_8_example_positive_negative_predict_5_27_table d
on a.user_id=d.user_id and a.brand_id=d.brand_id
left outer join wx1_6_75_9_example_positive_negative_predict_5_27_table e
on a.user_id=e.user_id and a.brand_id=e.brand_id
left outer join wx1_6_75_10_example_positive_negative_predict_5_27_table f
on a.user_id=f.user_id and a.brand_id=f.brand_id
left outer join wx1_6_75_11_example_positive_negative_predict_5_27_table g
on a.user_id=g.user_id and a.brand_id=g.brand_id""");

configs = [('firstOneTen_noBuy_click', 'null', '0'),('firstSecondTen_noBuy_click', 'null', '0'),
('firstThirdTen_noBuy_click', 'null', '0'),
('secondOneTen_noBuy_click', 'null', '0'),('secondSecondTen_noBuy_click', 'null', '0'),
('secondThirdTen_noBuy_click', 'null', '0')]
DataProc.fillMissingValues('wx1_6_76_5_example_positive_negative_predict_5_27_table', 
'wx1_6_76_5_example_positive_negative_predict_5_27_table123', configs);


#品牌特征
sql("""create table wx1_brand_lastMonth_train_table
as select brand_id,count(brand_id) as brand_lastMonthNum
from source_data
where type=1 and visit_datetime>="06-16" and visit_datetime<="07-15"
group by brand_id""");

sql("""create table wx1_7_6_5_example_positive_negative_train_5_27_table123
as select a.*,b.brand_lastMonthNum
from wx1_6_76_5_example_positive_negative_train_5_27_table123 a
left outer join wx1_brand_lastMonth_train_table b
on a.brand_id=b.brand_id""");

configs = [('brand_lastMonthNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_5_example_positive_negative_train_5_27_table123', 
'wx1_7_6_5_example_positive_negative_train_5_27_table12345', configs);

sql("""create table wx1_brand_lastMonth_predict_table
as select brand_id,count(brand_id) as brand_lastMonthNum
from source_data
where type=1 and visit_datetime>="07-16" and visit_datetime<="08-15"
group by brand_id""");

sql("""create table wx1_7_6_5_example_positive_negative_predict_5_27_table123
as select a.*,b.brand_lastMonthNum
from wx1_6_76_5_example_positive_negative_predict_5_27_table123 a
left outer join wx1_brand_lastMonth_predict_table b
on a.brand_id=b.brand_id""");

configs = [('brand_lastMonthNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_5_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_5_example_positive_negative_predict_5_27_table12345', configs);

#用户特征(训练)
sql("""create table wx1_user_last3click_train_table 
as select user_id,count(user_id) as user_last3clickNum
from source_data
where type=0 and visit_datetime>='07-13' and visit_datetime<='07-15'
group by user_id""");

sql("""create table wx1_user_last3buy_train_table 
as select user_id,count(user_id) as user_last3buyNum
from source_data
where type=1 and visit_datetime>='07-13' and visit_datetime<='07-15'
group by user_id""");

sql("""create table wx1_user_last3click_buy_train_table
as select a.*,b.user_last3buyNum
from wx1_user_last3click_train_table a
left outer join wx1_user_last3buy_train_table b
on a.user_id=b.user_id""");

configs = [('user_last3buyNum', 'null', '0')]
DataProc.fillMissingValues('wx1_user_last3click_buy_train_table', 
'wx1_user_last3click_buy_train_table123', configs);

sql("""create table wx1_user_last3click_nobuy_train_table123
as select user_id,user_last3clickNum 
from wx1_user_last3click_buy_train_table123
where user_last3buyNum=0""");

sql("""create table wx1_7_6_6_example_positive_negative_train_5_27_table123
as select a.*,b.user_last3clickNum
from wx1_7_6_5_example_positive_negative_train_5_27_table12345 a
left outer join wx1_user_last3click_nobuy_train_table123 b
on a.user_id=b.user_id""")

configs = [('user_last3clickNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_6_example_positive_negative_train_5_27_table123', 
'wx1_7_6_6_example_positive_negative_train_5_27_table12345', configs);
#用户特征(预测)
sql("""create table wx1_user_last3click_predict_table 
as select user_id,count(user_id) as user_last3clickNum
from source_data
where type=0 and visit_datetime>='08-13' and visit_datetime<='08-15'
group by user_id""");

sql("""create table wx1_user_last3buy_predict_table 
as select user_id,count(user_id) as user_last3buyNum
from source_data
where type=1 and visit_datetime>='08-13' and visit_datetime<='08-15'
group by user_id""");

sql("""create table wx1_user_last3click_buy_predict_table
as select a.*,b.user_last3buyNum
from wx1_user_last3click_predict_table a
left outer join wx1_user_last3buy_predict_table b
on a.user_id=b.user_id""");

configs = [('user_last3buyNum', 'null', '0')]
DataProc.fillMissingValues('wx1_user_last3click_buy_predict_table', 
'wx1_user_last3click_buy_predict_table123', configs);

sql("""create table wx1_user_last3click_nobuy_predict_table123
as select user_id,user_last3clickNum 
from wx1_user_last3click_buy_predict_table123
where user_last3buyNum=0""");

sql("""create table wx1_7_6_6_example_positive_negative_predict_5_27_table123
as select a.*,b.user_last3clickNum
from wx1_7_6_5_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_user_last3click_nobuy_predict_table123 b
on a.user_id=b.user_id""")

configs = [('user_last3clickNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_6_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_6_example_positive_negative_predict_5_27_table12345', configs);


#用户特征(训练)最后10天点击没购买
sql("""create table wx1_user_last10click_train_table 
as select user_id,count(user_id) as user_last10clickNum
from source_data
where type=0 and visit_datetime>='07-06' and visit_datetime<='07-15'
group by user_id""");

sql("""create table wx1_user_last10buy_train_table 
as select user_id,count(user_id) as user_last10buyNum
from source_data
where type=1 and visit_datetime>='07-06' and visit_datetime<='07-15'
group by user_id""");

sql("""create table wx1_user_last10click_buy_train_table
as select a.*,b.user_last10buyNum
from wx1_user_last10click_train_table a
left outer join wx1_user_last10buy_train_table b
on a.user_id=b.user_id""");

configs = [('user_last10buyNum', 'null', '0')]
DataProc.fillMissingValues('wx1_user_last10click_buy_train_table', 
'wx1_user_last10click_buy_train_table123', configs);

sql("""create table wx1_user_last10click_nobuy_train_table123
as select user_id,user_last10clickNum 
from wx1_user_last10click_buy_train_table123
where user_last10buyNum=0""");

sql("""create table wx1_7_6_7_example_positive_negative_train_5_27_table123
as select a.*,b.user_last10clickNum
from wx1_7_6_6_example_positive_negative_train_5_27_table12345 a
left outer join wx1_user_last10click_nobuy_train_table123 b
on a.user_id=b.user_id""")

configs = [('user_last10clickNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_7_example_positive_negative_train_5_27_table123', 
'wx1_7_6_7_example_positive_negative_train_5_27_table12345', configs);
#用户特征(预测)最后10天点击没购买
sql("""create table wx1_user_last10click_predict_table 
as select user_id,count(user_id) as user_last10clickNum
from source_data
where type=0 and visit_datetime>='08-06' and visit_datetime<='08-15'
group by user_id""");

sql("""create table wx1_user_last10buy_predict_table 
as select user_id,count(user_id) as user_last10buyNum
from source_data
where type=1 and visit_datetime>='08-06' and visit_datetime<='08-15'
group by user_id""");

sql("""create table wx1_user_last10click_buy_predict_table
as select a.*,b.user_last10buyNum
from wx1_user_last10click_predict_table a
left outer join wx1_user_last10buy_predict_table b
on a.user_id=b.user_id""");

configs = [('user_last10buyNum', 'null', '0')]
DataProc.fillMissingValues('wx1_user_last10click_buy_predict_table', 
'wx1_user_last10click_buy_predict_table123', configs);

sql("""create table wx1_user_last10click_nobuy_predict_table123
as select user_id,user_last10clickNum 
from wx1_user_last10click_buy_predict_table123
where user_last10buyNum=0""");

sql("""create table wx1_7_6_7_example_positive_negative_predict_5_27_table123
as select a.*,b.user_last10clickNum
from wx1_7_6_6_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_user_last10click_nobuy_predict_table123 b
on a.user_id=b.user_id""")

configs = [('user_last10clickNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_7_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_7_example_positive_negative_predict_5_27_table12345', configs);

#用户特征(训练)最后1天点击没购买
sql("""create table wx1_user_last1click_train_table 
as select user_id,count(user_id) as user_last1clickNum
from source_data
where type=0 and visit_datetime>='07-15' and visit_datetime<='07-15'
group by user_id""");

sql("""create table wx1_user_last1buy_train_table 
as select user_id,count(user_id) as user_last1buyNum
from source_data
where type=1 and visit_datetime>='07-15' and visit_datetime<='07-15'
group by user_id""");

sql("""create table wx1_user_last1click_buy_train_table
as select a.*,b.user_last1buyNum
from wx1_user_last1click_train_table a
left outer join wx1_user_last1buy_train_table b
on a.user_id=b.user_id""");

configs = [('user_last1buyNum', 'null', '0')]
DataProc.fillMissingValues('wx1_user_last1click_buy_train_table', 
'wx1_user_last1click_buy_train_table123', configs);

sql("""create table wx1_user_last1click_nobuy_train_table123
as select user_id,user_last1clickNum 
from wx1_user_last1click_buy_train_table123
where user_last1buyNum=0""");

sql("""create table wx1_7_6_8_example_positive_negative_train_5_27_table123
as select a.*,b.user_last1clickNum
from wx1_7_6_7_example_positive_negative_train_5_27_table12345 a
left outer join wx1_user_last1click_nobuy_train_table123 b
on a.user_id=b.user_id""")

configs = [('user_last1clickNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_8_example_positive_negative_train_5_27_table123', 
'wx1_7_6_8_example_positive_negative_train_5_27_table12345', configs);
#用户特征(预测)最后1天点击没购买
sql("""create table wx1_user_last1click_predict_table 
as select user_id,count(user_id) as user_last1clickNum
from source_data
where type=0 and visit_datetime>='08-15' and visit_datetime<='08-15'
group by user_id""");

sql("""create table wx1_user_last1buy_predict_table 
as select user_id,count(user_id) as user_last1buyNum
from source_data
where type=1 and visit_datetime>='08-15' and visit_datetime<='08-15'
group by user_id""");

sql("""create table wx1_user_last1click_buy_predict_table
as select a.*,b.user_last1buyNum
from wx1_user_last1click_predict_table a
left outer join wx1_user_last1buy_predict_table b
on a.user_id=b.user_id""");

configs = [('user_last1buyNum', 'null', '0')]
DataProc.fillMissingValues('wx1_user_last1click_buy_predict_table', 
'wx1_user_last1click_buy_predict_table123', configs);

sql("""create table wx1_user_last1click_nobuy_predict_table123
as select user_id,user_last1clickNum 
from wx1_user_last1click_buy_predict_table123
where user_last1buyNum=0""");

sql("""create table wx1_7_6_8_example_positive_negative_predict_5_27_table123
as select a.*,b.user_last1clickNum
from wx1_7_6_7_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_user_last1click_nobuy_predict_table123 b
on a.user_id=b.user_id""")

configs = [('user_last1clickNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_8_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_8_example_positive_negative_predict_5_27_table12345', configs);


#用户特征(训练)最后一个月和最后两个月购买量
sql("""create table wx1_user_last1monthbuy_train_table 
as select user_id,count(user_id) as user_last1monthbuyNum
from source_data
where type=1 and visit_datetime>='06-16' and visit_datetime<='07-15'
group by user_id""");

sql("""create table wx1_7_6_9_example_positive_negative_train_5_27_table123
as select a.*,b.user_last1monthbuyNum
from wx1_7_6_8_example_positive_negative_train_5_27_table12345 a
left outer join wx1_user_last1monthbuy_train_table b
on a.user_id=b.user_id""")

configs = [('user_last1monthbuyNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_9_example_positive_negative_train_5_27_table123', 
'wx1_7_6_9_example_positive_negative_train_5_27_table12345', configs);

sql("""create table wx1_user_last2monthbuy_train_table 
as select user_id,count(user_id) as user_last2monthbuyNum
from source_data
where type=1 and visit_datetime>='05-15' and visit_datetime<='06-15'
group by user_id""");

sql("""create table wx1_7_6_10_example_positive_negative_train_5_27_table123
as select a.*,b.user_last2monthbuyNum
from wx1_7_6_9_example_positive_negative_train_5_27_table12345 a
left outer join wx1_user_last2monthbuy_train_table b
on a.user_id=b.user_id""")

configs = [('user_last2monthbuyNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_10_example_positive_negative_train_5_27_table123', 
'wx1_7_6_10_example_positive_negative_train_5_27_table12345', configs);

sql("""create table wx1_user_last3monthbuy_train_table 
as select user_id,count(user_id) as user_last3monthbuyNum
from source_data
where type=1 and visit_datetime>='04-15' and visit_datetime<='05-15'
group by user_id""");

sql("""create table wx1_7_6_11_example_positive_negative_train_5_27_table123
as select a.*,b.user_last3monthbuyNum
from wx1_7_6_10_example_positive_negative_train_5_27_table12345 a
left outer join wx1_user_last3monthbuy_train_table b
on a.user_id=b.user_id""")

configs = [('user_last3monthbuyNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_11_example_positive_negative_train_5_27_table123', 
'wx1_7_6_11_example_positive_negative_train_5_27_table12345', configs);

#用户特征(预测)最后一个月和最后两个月购买量
sql("""create table wx1_user_last1monthbuy_predict_table 
as select user_id,count(user_id) as user_last1monthbuyNum
from source_data
where type=1 and visit_datetime>='07-16' and visit_datetime<='08-15'
group by user_id""");

sql("""create table wx1_7_6_9_example_positive_negative_predict_5_27_table123
as select a.*,b.user_last1monthbuyNum
from wx1_7_6_8_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_user_last1monthbuy_predict_table b
on a.user_id=b.user_id""")

configs = [('user_last1monthbuyNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_9_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_9_example_positive_negative_predict_5_27_table12345', configs);

sql("""create table wx1_user_last2monthbuy_predict_table 
as select user_id,count(user_id) as user_last2monthbuyNum
from source_data
where type=1 and visit_datetime>='06-15' and visit_datetime<='07-15'
group by user_id""");

sql("""create table wx1_7_6_10_example_positive_negative_predict_5_27_table123
as select a.*,b.user_last2monthbuyNum
from wx1_7_6_9_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_user_last2monthbuy_predict_table b
on a.user_id=b.user_id""")

configs = [('user_last2monthbuyNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_10_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_10_example_positive_negative_predict_5_27_table12345', configs);

sql("""create table wx1_user_last3monthbuy_predict_table 
as select user_id,count(user_id) as user_last3monthbuyNum
from source_data
where type=1 and visit_datetime>='05-16' and visit_datetime<='06-15'
group by user_id""");

sql("""create table wx1_7_6_11_example_positive_negative_predict_5_27_table123
as select a.*,b.user_last3monthbuyNum
from wx1_7_6_10_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_user_last3monthbuy_predict_table b
on a.user_id=b.user_id""")

configs = [('user_last3monthbuyNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_11_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_11_example_positive_negative_predict_5_27_table12345', configs);

#训练阶段，每个用户购买频率
sql("""create table wx1_user_diffDaysbuy_train_table as 
       select user_id,count(distinct visit_datetime) as ratebuy_days
	   from source_data where type=1
	   and visit_datetime>="04-15" and visit_datetime<="07-15"
	   group by user_id""");

sql("""create table wx1_7_6_12_example_positive_negative_train_5_27_table123
as select a.*,b.ratebuy_days
from wx1_7_6_11_example_positive_negative_train_5_27_table12345 a
left outer join wx1_user_diffDaysbuy_train_table b
on a.user_id=b.user_id""")

configs = [('ratebuy_days', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_12_example_positive_negative_train_5_27_table123', 
'wx1_7_6_12_example_positive_negative_train_5_27_table12345', configs);
	   
#预测阶段，每个用户购买频率
sql("""create table wx1_user_diffDaysbuy_predict_table as 
       select user_id,count(distinct visit_datetime) as ratebuy_days
	   from source_data where type=1
	   and visit_datetime>="05-16" and visit_datetime<="08-15"
	   group by user_id""");
	   
sql("""create table wx1_7_6_12_example_positive_negative_predict_5_27_table123
as select a.*,b.ratebuy_days
from wx1_7_6_11_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_user_diffDaysbuy_predict_table b
on a.user_id=b.user_id""")

configs = [('ratebuy_days', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_12_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_12_example_positive_negative_predict_5_27_table12345', configs);


#训练阶段，每个用户每个月的购买频率
sql("""create table wx1_user_last1monthdiffDaysbuy_train_table as 
       select user_id,count(distinct visit_datetime) as last1monthratebuy_days
	   from source_data where type=1
	   and visit_datetime>="06-16" and visit_datetime<="07-15"
	   group by user_id""");

sql("""create table wx1_7_6_13_example_positive_negative_train_5_27_table123
as select a.*,b.last1monthratebuy_days
from wx1_7_6_12_example_positive_negative_train_5_27_table12345 a
left outer join wx1_user_last1monthdiffDaysbuy_train_table b
on a.user_id=b.user_id""")

configs = [('last1monthratebuy_days', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_13_example_positive_negative_train_5_27_table123', 
'wx1_7_6_13_example_positive_negative_train_5_27_table12345', configs);

sql("""create table wx1_user_last2monthdiffDaysbuy_train_table as 
       select user_id,count(distinct visit_datetime) as last2monthratebuy_days
	   from source_data where type=1
	   and visit_datetime>="05-16" and visit_datetime<="06-15"
	   group by user_id""");

sql("""create table wx1_7_6_14_example_positive_negative_train_5_27_table123
as select a.*,b.last2monthratebuy_days
from wx1_7_6_13_example_positive_negative_train_5_27_table12345 a
left outer join wx1_user_last2monthdiffDaysbuy_train_table b
on a.user_id=b.user_id""")

configs = [('last2monthratebuy_days', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_14_example_positive_negative_train_5_27_table123', 
'wx1_7_6_14_example_positive_negative_train_5_27_table12345', configs);

sql("""create table wx1_user_last3monthdiffDaysbuy_train_table as 
       select user_id,count(distinct visit_datetime) as last3monthratebuy_days
	   from source_data where type=1
	   and visit_datetime>="04-15" and visit_datetime<="05-15"
	   group by user_id""");

sql("""create table wx1_7_6_15_example_positive_negative_train_5_27_table123
as select a.*,b.last3monthratebuy_days
from wx1_7_6_14_example_positive_negative_train_5_27_table12345 a
left outer join wx1_user_last3monthdiffDaysbuy_train_table b
on a.user_id=b.user_id""")

configs = [('last3monthratebuy_days', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_15_example_positive_negative_train_5_27_table123', 
'wx1_7_6_15_example_positive_negative_train_5_27_table12345', configs);
	   
#预测阶段,每个用户每个月的购买频率
sql("""create table wx1_user_last1monthdiffDaysbuy_predict_table as 
       select user_id,count(distinct visit_datetime) as last1monthratebuy_days
	   from source_data where type=1
	   and visit_datetime>="07-16" and visit_datetime<="08-15"
	   group by user_id""");

sql("""create table wx1_7_6_13_example_positive_negative_predict_5_27_table123
as select a.*,b.last1monthratebuy_days
from wx1_7_6_12_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_user_last1monthdiffDaysbuy_predict_table b
on a.user_id=b.user_id""")

configs = [('last1monthratebuy_days', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_13_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_13_example_positive_negative_predict_5_27_table12345', configs);

sql("""create table wx1_user_last2monthdiffDaysbuy_predict_table as 
       select user_id,count(distinct visit_datetime) as last2monthratebuy_days
	   from source_data where type=1
	   and visit_datetime>="06-16" and visit_datetime<="07-15"
	   group by user_id""");

sql("""create table wx1_7_6_14_example_positive_negative_predict_5_27_table123
as select a.*,b.last2monthratebuy_days
from wx1_7_6_13_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_user_last2monthdiffDaysbuy_predict_table b
on a.user_id=b.user_id""")

configs = [('last2monthratebuy_days', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_14_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_14_example_positive_negative_predict_5_27_table12345', configs);

sql("""create table wx1_user_last3monthdiffDaysbuy_predict_table as 
       select user_id,count(distinct visit_datetime) as last3monthratebuy_days
	   from source_data where type=1
	   and visit_datetime>="05-16" and visit_datetime<="06-15"
	   group by user_id""");

sql("""create table wx1_7_6_15_example_positive_negative_predict_5_27_table123
as select a.*,b.last3monthratebuy_days
from wx1_7_6_14_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_user_last3monthdiffDaysbuy_predict_table b
on a.user_id=b.user_id""")

configs = [('last3monthratebuy_days', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_15_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_15_example_positive_negative_predict_5_27_table12345', configs);


#品牌特征(训练最后第二个月被购买的品牌量)
sql("""create table wx1_brand_last2Month_train_table
as select brand_id,count(brand_id) as brand_last2MonthNum
from source_data
where type=1 and visit_datetime>="05-16" and visit_datetime<="06-15"
group by brand_id""");

sql("""create table wx1_7_6_16_example_positive_negative_train_5_27_table123
as select a.*,b.brand_last2MonthNum
from wx1_7_6_15_example_positive_negative_train_5_27_table12345 a
left outer join wx1_brand_last2Month_train_table b
on a.brand_id=b.brand_id""");

configs = [('brand_last2MonthNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_16_example_positive_negative_train_5_27_table123', 
'wx1_7_6_16_example_positive_negative_train_5_27_table12345', configs);
#品牌特征(训练最后第三个月被购买的品牌量)
sql("""create table wx1_brand_last3Month_train_table
as select brand_id,count(brand_id) as brand_last3MonthNum
from source_data
where type=1 and visit_datetime>="04-15" and visit_datetime<="05-15"
group by brand_id""");

sql("""create table wx1_7_6_17_example_positive_negative_train_5_27_table123
as select a.*,b.brand_last3MonthNum
from wx1_7_6_16_example_positive_negative_train_5_27_table12345 a
left outer join wx1_brand_last3Month_train_table b
on a.brand_id=b.brand_id""");

configs = [('brand_last3MonthNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_17_example_positive_negative_train_5_27_table123', 
'wx1_7_6_17_example_positive_negative_train_5_27_table12345', configs);

#品牌特征(预测最后第二个月被购买的品牌量)
sql("""create table wx1_brand_last2Month_predict_table
as select brand_id,count(brand_id) as brand_last2MonthNum
from source_data
where type=1 and visit_datetime>="06-16" and visit_datetime<="07-15"
group by brand_id""");

sql("""create table wx1_7_6_16_example_positive_negative_predict_5_27_table123
as select a.*,b.brand_last2MonthNum
from wx1_7_6_15_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_brand_last2Month_predict_table b
on a.brand_id=b.brand_id""");

configs = [('brand_last2MonthNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_16_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_16_example_positive_negative_predict_5_27_table12345', configs);
#品牌特征(预测最后第三个月被购买的品牌量)
sql("""create table wx1_brand_last3Month_predict_table
as select brand_id,count(brand_id) as brand_last3MonthNum
from source_data
where type=1 and visit_datetime>="05-16" and visit_datetime<="06-15"
group by brand_id""");

sql("""create table wx1_7_6_17_example_positive_negative_predict_5_27_table123
as select a.*,b.brand_last3MonthNum
from wx1_7_6_16_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_brand_last3Month_predict_table b
on a.brand_id=b.brand_id""");

configs = [('brand_last3MonthNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_17_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_17_example_positive_negative_predict_5_27_table12345', configs);

#品牌特征(最后1天被购买的品牌量)
sql("""create table wx1_brand_last1Days_train_table
as select brand_id,count(brand_id) as brand_last1DaysNum
from source_data
where type=1 and visit_datetime>="07-15" and visit_datetime<="07-15"
group by brand_id""");

sql("""create table wx1_7_6_18_example_positive_negative_train_5_27_table123
as select a.*,b.brand_last1DaysNum
from wx1_7_6_17_example_positive_negative_train_5_27_table12345 a
left outer join wx1_brand_last1Days_train_table b
on a.brand_id=b.brand_id""");

configs = [('brand_last1DaysNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_18_example_positive_negative_train_5_27_table123', 
'wx1_7_6_18_example_positive_negative_train_5_27_table12345', configs);

sql("""create table wx1_brand_last1Days_predict_table
as select brand_id,count(brand_id) as brand_last1DaysNum
from source_data
where type=1 and visit_datetime>="08-15" and visit_datetime<="08-15"
group by brand_id""");

sql("""create table wx1_7_6_18_example_positive_negative_predict_5_27_table123
as select a.*,b.brand_last1DaysNum
from wx1_7_6_17_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_brand_last1Days_predict_table b
on a.brand_id=b.brand_id""");

configs = [('brand_last1DaysNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_18_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_18_example_positive_negative_predict_5_27_table12345', configs);

#品牌特征(最后3天被购买的品牌量)
sql("""create table wx1_brand_last3Days_train_table
as select brand_id,count(brand_id) as brand_last3DaysNum
from source_data
where type=1 and visit_datetime>="07-13" and visit_datetime<="07-15"
group by brand_id""");

sql("""create table wx1_7_6_19_example_positive_negative_train_5_27_table123
as select a.*,b.brand_last3DaysNum
from wx1_7_6_18_example_positive_negative_train_5_27_table12345 a
left outer join wx1_brand_last3Days_train_table b
on a.brand_id=b.brand_id""");

configs = [('brand_last3DaysNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_19_example_positive_negative_train_5_27_table123', 
'wx1_7_6_19_example_positive_negative_train_5_27_table12345', configs);

sql("""create table wx1_brand_last3Days_predict_table
as select brand_id,count(brand_id) as brand_last3DaysNum
from source_data
where type=1 and visit_datetime>="08-13" and visit_datetime<="08-15"
group by brand_id""");

sql("""create table wx1_7_6_19_example_positive_negative_predict_5_27_table123
as select a.*,b.brand_last3DaysNum
from wx1_7_6_18_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_brand_last3Days_predict_table b
on a.brand_id=b.brand_id""");

configs = [('brand_last3DaysNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_19_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_19_example_positive_negative_predict_5_27_table12345', configs);

#品牌特征(最后7天被购买的品牌量)
sql("""create table wx1_brand_last7Days_train_table
as select brand_id,count(brand_id) as brand_last7DaysNum
from source_data
where type=1 and visit_datetime>="07-09" and visit_datetime<="07-15"
group by brand_id""");

sql("""create table wx1_7_6_20_example_positive_negative_train_5_27_table123
as select a.*,b.brand_last7DaysNum
from wx1_7_6_19_example_positive_negative_train_5_27_table12345 a
left outer join wx1_brand_last7Days_train_table b
on a.brand_id=b.brand_id""");

configs = [('brand_last7DaysNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_20_example_positive_negative_train_5_27_table123', 
'wx1_7_6_20_example_positive_negative_train_5_27_table12345', configs);

sql("""create table wx1_brand_last7Days_predict_table
as select brand_id,count(brand_id) as brand_last7DaysNum
from source_data
where type=1 and visit_datetime>="08-09" and visit_datetime<="08-15"
group by brand_id""");

sql("""create table wx1_7_6_20_example_positive_negative_predict_5_27_table123
as select a.*,b.brand_last7DaysNum
from wx1_7_6_19_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_brand_last7Days_predict_table b
on a.brand_id=b.brand_id""");

configs = [('brand_last7DaysNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_20_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_20_example_positive_negative_predict_5_27_table12345', configs);

#品牌特征(最后1天被点击的品牌量)
sql("""create table wx1_brand_last1Daysclick_train_table
as select brand_id,count(brand_id) as brand_last1DaysclickNum
from source_data
where type=0 and visit_datetime>="07-15" and visit_datetime<="07-15"
group by brand_id""");

sql("""create table wx1_7_6_21_example_positive_negative_train_5_27_table123
as select a.*,b.brand_last1DaysclickNum
from wx1_7_6_20_example_positive_negative_train_5_27_table12345 a
left outer join wx1_brand_last1Daysclick_train_table b
on a.brand_id=b.brand_id""");

configs = [('brand_last1DaysclickNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_21_example_positive_negative_train_5_27_table123', 
'wx1_7_6_21_example_positive_negative_train_5_27_table12345', configs);

sql("""create table wx1_brand_last1Daysclick_predict_table
as select brand_id,count(brand_id) as brand_last1DaysclickNum
from source_data
where type=0 and visit_datetime>="08-15" and visit_datetime<="08-15"
group by brand_id""");

sql("""create table wx1_7_6_21_example_positive_negative_predict_5_27_table123
as select a.*,b.brand_last1DaysclickNum
from wx1_7_6_20_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_brand_last1Daysclick_predict_table b
on a.brand_id=b.brand_id""");

configs = [('brand_last1DaysclickNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_21_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_21_example_positive_negative_predict_5_27_table12345', configs);

#品牌特征(最后3天被点击的品牌量)
sql("""create table wx1_brand_last3Daysclick_train_table
as select brand_id,count(brand_id) as brand_last3DaysclickNum
from source_data
where type=0 and visit_datetime>="07-13" and visit_datetime<="07-15"
group by brand_id""");

sql("""create table wx1_7_6_22_example_positive_negative_train_5_27_table123
as select a.*,b.brand_last3DaysclickNum
from wx1_7_6_21_example_positive_negative_train_5_27_table12345 a
left outer join wx1_brand_last3Daysclick_train_table b
on a.brand_id=b.brand_id""");

configs = [('brand_last3DaysclickNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_22_example_positive_negative_train_5_27_table123', 
'wx1_7_6_22_example_positive_negative_train_5_27_table12345', configs);

sql("""create table wx1_brand_last3Daysclick_predict_table
as select brand_id,count(brand_id) as brand_last3DaysclickNum
from source_data
where type=0 and visit_datetime>="08-13" and visit_datetime<="08-15"
group by brand_id""");

sql("""create table wx1_7_6_22_example_positive_negative_predict_5_27_table123
as select a.*,b.brand_last3DaysclickNum
from wx1_7_6_21_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_brand_last3Daysclick_predict_table b
on a.brand_id=b.brand_id""");

configs = [('brand_last3DaysclickNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_22_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_22_example_positive_negative_predict_5_27_table12345', configs);


#品牌特征(最后7天被点击的品牌量)
sql("""create table wx1_brand_last7Daysclick_train_table
as select brand_id,count(brand_id) as brand_last7DaysclickNum
from source_data
where type=0 and visit_datetime>="07-09" and visit_datetime<="07-15"
group by brand_id""");

sql("""create table wx1_7_6_23_example_positive_negative_train_5_27_table123
as select a.*,b.brand_last7DaysclickNum
from wx1_7_6_22_example_positive_negative_train_5_27_table12345 a
left outer join wx1_brand_last7Daysclick_train_table b
on a.brand_id=b.brand_id""");

configs = [('brand_last7DaysclickNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_23_example_positive_negative_train_5_27_table123', 
'wx1_7_6_23_example_positive_negative_train_5_27_table12345', configs);

sql("""create table wx1_brand_last7Daysclick_predict_table
as select brand_id,count(brand_id) as brand_last7DaysclickNum
from source_data
where type=0 and visit_datetime>="08-09" and visit_datetime<="08-15"
group by brand_id""");

sql("""create table wx1_7_6_23_example_positive_negative_predict_5_27_table123
as select a.*,b.brand_last7DaysclickNum
from wx1_7_6_22_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_brand_last7Daysclick_predict_table b
on a.brand_id=b.brand_id""");

configs = [('brand_last7DaysclickNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_23_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_23_example_positive_negative_predict_5_27_table12345', configs);

#新加

#用户特征(训练)最后一个月和最后两个月和最后三个月的收藏量
sql("""create table wx1_user_last1monthcollect_train_table 
as select user_id,count(user_id) as user_last1monthcollectNum
from source_data
where type=2 and visit_datetime>='06-16' and visit_datetime<='07-15'
group by user_id""");

sql("""create table wx1_7_6_24_example_positive_negative_train_5_27_table123
as select a.*,b.user_last1monthcollectNum
from wx1_7_6_23_example_positive_negative_train_5_27_table12345 a
left outer join wx1_user_last1monthcollect_train_table b
on a.user_id=b.user_id""")

configs = [('user_last1monthcollectNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_24_example_positive_negative_train_5_27_table123', 
'wx1_7_6_24_example_positive_negative_train_5_27_table12345', configs);

sql("""create table wx1_user_last2monthcollect_train_table 
as select user_id,count(user_id) as user_last2monthcollectNum
from source_data
where type=2 and visit_datetime>='05-16' and visit_datetime<='06-15'
group by user_id""");

sql("""create table wx1_7_6_25_example_positive_negative_train_5_27_table123
as select a.*,b.user_last2monthcollectNum
from wx1_7_6_24_example_positive_negative_train_5_27_table12345 a
left outer join wx1_user_last2monthcollect_train_table b
on a.user_id=b.user_id""")

configs = [('user_last2monthcollectNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_25_example_positive_negative_train_5_27_table123', 
'wx1_7_6_25_example_positive_negative_train_5_27_table12345', configs);

sql("""create table wx1_user_last3monthcollect_train_table 
as select user_id,count(user_id) as user_last3monthcollectNum
from source_data
where type=2 and visit_datetime>='04-15' and visit_datetime<='05-15'
group by user_id""");

sql("""create table wx1_7_6_26_example_positive_negative_train_5_27_table123
as select a.*,b.user_last3monthcollectNum
from wx1_7_6_25_example_positive_negative_train_5_27_table12345 a
left outer join wx1_user_last3monthcollect_train_table b
on a.user_id=b.user_id""")

configs = [('user_last3monthcollectNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_26_example_positive_negative_train_5_27_table123', 
'wx1_7_6_26_example_positive_negative_train_5_27_table12345', configs);

#用户特征(预测)最后一个月和最后两个月和最后三个月收藏量
sql("""create table wx1_user_last1monthcollect_predict_table 
as select user_id,count(user_id) as user_last1monthcollectNum
from source_data
where type=2 and visit_datetime>='07-16' and visit_datetime<='08-15'
group by user_id""");

sql("""create table wx1_7_6_24_example_positive_negative_predict_5_27_table123
as select a.*,b.user_last1monthcollectNum
from wx1_7_6_23_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_user_last1monthcollect_predict_table b
on a.user_id=b.user_id""")

configs = [('user_last1monthcollectNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_24_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_24_example_positive_negative_predict_5_27_table12345', configs);

sql("""create table wx1_user_last2monthcollect_predict_table 
as select user_id,count(user_id) as user_last2monthcollectNum
from source_data
where type=2 and visit_datetime>='06-16' and visit_datetime<='07-15'
group by user_id""");

sql("""create table wx1_7_6_25_example_positive_negative_predict_5_27_table123
as select a.*,b.user_last2monthcollectNum
from wx1_7_6_24_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_user_last2monthcollect_predict_table b
on a.user_id=b.user_id""")

configs = [('user_last2monthcollectNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_25_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_25_example_positive_negative_predict_5_27_table12345', configs);

sql("""create table wx1_user_last3monthcollect_predict_table 
as select user_id,count(user_id) as user_last3monthcollectNum
from source_data
where type=2 and visit_datetime>='05-16' and visit_datetime<='06-15'
group by user_id""");

sql("""create table wx1_7_6_26_example_positive_negative_predict_5_27_table123
as select a.*,b.user_last3monthcollectNum
from wx1_7_6_25_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_user_last3monthcollect_predict_table b
on a.user_id=b.user_id""")

configs = [('user_last3monthcollectNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_26_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_26_example_positive_negative_predict_5_27_table12345', configs);



#用户特征(训练)最后一个月和最后两个月和最后三个月的加购物车量
sql("""create table wx1_user_last1monthcart_train_table 
as select user_id,count(user_id) as user_last1monthcartNum
from source_data
where type=3 and visit_datetime>='06-16' and visit_datetime<='07-15'
group by user_id""");

sql("""create table wx1_7_6_27_example_positive_negative_train_5_27_table123
as select a.*,b.user_last1monthcartNum
from wx1_7_6_26_example_positive_negative_train_5_27_table12345 a
left outer join wx1_user_last1monthcart_train_table b
on a.user_id=b.user_id""")

configs = [('user_last1monthcartNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_27_example_positive_negative_train_5_27_table123', 
'wx1_7_6_27_example_positive_negative_train_5_27_table12345', configs);

sql("""create table wx1_user_last2monthcart_train_table 
as select user_id,count(user_id) as user_last2monthcartNum
from source_data
where type=3 and visit_datetime>='05-16' and visit_datetime<='06-15'
group by user_id""");

sql("""create table wx1_7_6_28_example_positive_negative_train_5_27_table123
as select a.*,b.user_last2monthcartNum
from wx1_7_6_27_example_positive_negative_train_5_27_table12345 a
left outer join wx1_user_last2monthcart_train_table b
on a.user_id=b.user_id""")

configs = [('user_last2monthcartNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_28_example_positive_negative_train_5_27_table123', 
'wx1_7_6_28_example_positive_negative_train_5_27_table12345', configs);

sql("""create table wx1_user_last3monthcart_train_table 
as select user_id,count(user_id) as user_last3monthcartNum
from source_data
where type=3 and visit_datetime>='04-15' and visit_datetime<='05-15'
group by user_id""");

sql("""create table wx1_7_6_29_example_positive_negative_train_5_27_table123
as select a.*,b.user_last3monthcartNum
from wx1_7_6_28_example_positive_negative_train_5_27_table12345 a
left outer join wx1_user_last3monthcart_train_table b
on a.user_id=b.user_id""")

configs = [('user_last3monthcartNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_29_example_positive_negative_train_5_27_table123', 
'wx1_7_6_29_example_positive_negative_train_5_27_table12345', configs);

#用户特征(预测)最后一个月和最后两个月和最后三个月加购物车量
sql("""create table wx1_user_last1monthcart_predict_table 
as select user_id,count(user_id) as user_last1monthcartNum
from source_data
where type=3 and visit_datetime>='07-16' and visit_datetime<='08-15'
group by user_id""");

sql("""create table wx1_7_6_27_example_positive_negative_predict_5_27_table123
as select a.*,b.user_last1monthcartNum
from wx1_7_6_26_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_user_last1monthcart_predict_table b
on a.user_id=b.user_id""")

configs = [('user_last1monthcartNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_27_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_27_example_positive_negative_predict_5_27_table12345', configs);

sql("""create table wx1_user_last2monthcart_predict_table 
as select user_id,count(user_id) as user_last2monthcartNum
from source_data
where type=3 and visit_datetime>='06-16' and visit_datetime<='07-15'
group by user_id""");

sql("""create table wx1_7_6_28_example_positive_negative_predict_5_27_table123
as select a.*,b.user_last2monthcartNum
from wx1_7_6_27_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_user_last2monthcart_predict_table b
on a.user_id=b.user_id""")

configs = [('user_last2monthcartNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_28_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_28_example_positive_negative_predict_5_27_table12345', configs);

sql("""create table wx1_user_last3monthcart_predict_table 
as select user_id,count(user_id) as user_last3monthcartNum
from source_data
where type=3 and visit_datetime>='05-16' and visit_datetime<='06-15'
group by user_id""");

sql("""create table wx1_7_6_29_example_positive_negative_predict_5_27_table123
as select a.*,b.user_last3monthcartNum
from wx1_7_6_28_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_user_last3monthcart_predict_table b
on a.user_id=b.user_id""")

configs = [('user_last3monthcartNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_29_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_29_example_positive_negative_predict_5_27_table12345', configs);


#用户特征(训练)总的购买量
sql("""create table wx1_user_totalbuy_train_table 
as select user_id,count(user_id) as user_totalbuyNum
from source_data
where type=1 and visit_datetime>='04-15' and visit_datetime<='07-15'
group by user_id""");

sql("""create table wx1_7_6_30_example_positive_negative_train_5_27_table123
as select a.*,b.user_totalbuyNum
from wx1_7_6_29_example_positive_negative_train_5_27_table12345 a
left outer join wx1_user_totalbuy_train_table b
on a.user_id=b.user_id""")

configs = [('user_totalbuyNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_30_example_positive_negative_train_5_27_table123', 
'wx1_7_6_30_example_positive_negative_train_5_27_table12345', configs);

#用户特征(预测)总的购买量
sql("""create table wx1_user_totalbuy_predict_table 
as select user_id,count(user_id) as user_totalbuyNum
from source_data
where type=1 and visit_datetime>='05-16' and visit_datetime<='08-15'
group by user_id""");

sql("""create table wx1_7_6_30_example_positive_negative_predict_5_27_table123
as select a.*,b.user_totalbuyNum
from wx1_7_6_29_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_user_totalbuy_predict_table b
on a.user_id=b.user_id""")

configs = [('user_totalbuyNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_30_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_30_example_positive_negative_predict_5_27_table12345', configs);

#用户特征(训练)总的点击量
sql("""create table wx1_user_totalclick_train_table 
as select user_id,count(user_id) as user_totalclickNum
from source_data
where type=0 and visit_datetime>='04-15' and visit_datetime<='07-15'
group by user_id""");

sql("""create table wx1_7_6_31_example_positive_negative_train_5_27_table123
as select a.*,b.user_totalclickNum
from wx1_7_6_30_example_positive_negative_train_5_27_table12345 a
left outer join wx1_user_totalclick_train_table b
on a.user_id=b.user_id""")

configs = [('user_totalclickNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_31_example_positive_negative_train_5_27_table123', 
'wx1_7_6_31_example_positive_negative_train_5_27_table12345', configs);

#用户特征(预测)总的点击量
sql("""create table wx1_user_totalclick_predict_table 
as select user_id,count(user_id) as user_totalclickNum
from source_data
where type=0 and visit_datetime>='05-16' and visit_datetime<='08-15'
group by user_id""");

sql("""create table wx1_7_6_31_example_positive_negative_predict_5_27_table123
as select a.*,b.user_totalclickNum
from wx1_7_6_30_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_user_totalclick_predict_table b
on a.user_id=b.user_id""")

configs = [('user_totalclickNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_31_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_31_example_positive_negative_predict_5_27_table12345', configs);

#品牌特征(训练)总的购买量
sql("""create table wx1_brand_totalbuy_train_table 
as select brand_id,count(brand_id) as brand_totalbuyNum
from source_data
where type=1 and visit_datetime>='04-15' and visit_datetime<='07-15'
group by brand_id""");

sql("""create table wx1_7_6_32_example_positive_negative_train_5_27_table123
as select a.*,b.brand_totalbuyNum
from wx1_7_6_31_example_positive_negative_train_5_27_table12345 a
left outer join wx1_brand_totalbuy_train_table b
on a.brand_id=b.brand_id""")

configs = [('brand_totalbuyNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_32_example_positive_negative_train_5_27_table123', 
'wx1_7_6_32_example_positive_negative_train_5_27_table12345', configs);

#品牌特征(预测)总的购买量
sql("""create table wx1_brand_totalbuy_predict_table 
as select brand_id,count(brand_id) as brand_totalbuyNum
from source_data
where type=1 and visit_datetime>='05-16' and visit_datetime<='08-15'
group by brand_id""");

sql("""create table wx1_7_6_32_example_positive_negative_predict_5_27_table123
as select a.*,b.brand_totalbuyNum
from wx1_7_6_31_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_brand_totalbuy_predict_table b
on a.brand_id=b.brand_id""")

configs = [('brand_totalbuyNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_32_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_32_example_positive_negative_predict_5_27_table12345', configs);

#品牌特征(训练)总的点击量
sql("""create table wx1_brand_totalclick_train_table 
as select brand_id,count(brand_id) as brand_totalclickNum
from source_data
where type=0 and visit_datetime>='04-15' and visit_datetime<='07-15'
group by brand_id""");

sql("""create table wx1_7_6_33_example_positive_negative_train_5_27_table123
as select a.*,b.brand_totalclickNum
from wx1_7_6_32_example_positive_negative_train_5_27_table12345 a
left outer join wx1_brand_totalclick_train_table b
on a.brand_id=b.brand_id""")

configs = [('brand_totalclickNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_33_example_positive_negative_train_5_27_table123', 
'wx1_7_6_33_example_positive_negative_train_5_27_table12345', configs);

#品牌特征(预测)总的点击量
sql("""create table wx1_brand_totalclick_predict_table 
as select brand_id,count(brand_id) as brand_totalclickNum
from source_data
where type=0 and visit_datetime>='05-16' and visit_datetime<='08-15'
group by brand_id""");

sql("""create table wx1_7_6_33_example_positive_negative_predict_5_27_table123
as select a.*,b.brand_totalclickNum
from wx1_7_6_32_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_brand_totalclick_predict_table b
on a.brand_id=b.brand_id""")

configs = [('brand_totalclickNum', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_33_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_33_example_positive_negative_predict_5_27_table12345', configs);

#用户品牌对特征(训练)总的购买量
sql("""create table wx1_7_6_34_example_positive_negative_train_5_27_table12345
as select *,buyNum1+buyNum2 as buyNum
from wx1_7_6_33_example_positive_negative_train_5_27_table12345""")
#用户品牌对特征(预测)总的购买量
sql("""create table wx1_7_6_34_example_positive_negative_predict_5_27_table12345
as select *,buyNum1+buyNum2 as buyNum
from wx1_7_6_33_example_positive_negative_predict_5_27_table12345""")

#用户品牌对特征(训练)总的点击量
sql("""create table wx1_7_6_35_example_positive_negative_train_5_27_table12345
as select *,clickNum1+clickNum2 as clickNum
from wx1_7_6_34_example_positive_negative_train_5_27_table12345""")
#用户品牌对特征(预测)总的点击量
sql("""create table wx1_7_6_35_example_positive_negative_predict_5_27_table12345
as select *,clickNum1+clickNum2 as clickNum
from wx1_7_6_34_example_positive_negative_predict_5_27_table12345""")


#构造综合特征
sql("""create table wx1_7_6_36_example_positive_negative_train_5_27_table12345
as select *,
case 
when buyNum=0 then -1 else clickNum/buyNum
end as user_brand_click_buy_pro,
case 
when brand_totalbuyNum=0 then -1 else brand_totalclickNum/brand_totalbuyNum
end as brand_click_buy_pro,
case 
when user_totalbuyNum=0 then -1 else user_totalclickNum/user_totalbuyNum
end as user_click_buy_pro
from wx1_7_6_35_example_positive_negative_train_5_27_table12345""")

sql("""create table wx1_7_6_36_example_positive_negative_predict_5_27_table12345
as select *,
case 
when buyNum=0 then -1 else clickNum/buyNum
end as user_brand_click_buy_pro,
case 
when brand_totalbuyNum=0 then -1 else brand_totalclickNum/brand_totalbuyNum
end as brand_click_buy_pro,
case 
when user_totalbuyNum=0 then -1 else user_totalclickNum/user_totalbuyNum
end as user_click_buy_pro
from wx1_7_6_35_example_positive_negative_predict_5_27_table12345""")



#训练阶段，每个用户点击频率
sql("""create table wx1_user_diffDaysclick_train_table as 
       select user_id,count(distinct visit_datetime) as rateclick_days
	   from source_data where type=0
	   and visit_datetime>="04-15" and visit_datetime<="07-15"
	   group by user_id""");

sql("""create table wx1_7_6_37_example_positive_negative_train_5_27_table123
as select a.*,b.rateclick_days
from wx1_7_6_36_example_positive_negative_train_5_27_table12345 a
left outer join wx1_user_diffDaysclick_train_table b
on a.user_id=b.user_id""")

configs = [('rateclick_days', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_37_example_positive_negative_train_5_27_table123', 
'wx1_7_6_37_example_positive_negative_train_5_27_table12345', configs);
	   
#预测阶段，每个用户点击频率
sql("""create table wx1_user_diffDaysclick_predict_table as 
       select user_id,count(distinct visit_datetime) as rateclick_days
	   from source_data where type=0
	   and visit_datetime>="05-16" and visit_datetime<="08-15"
	   group by user_id""");
	   
sql("""create table wx1_7_6_37_example_positive_negative_predict_5_27_table123
as select a.*,b.rateclick_days
from wx1_7_6_36_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_user_diffDaysclick_predict_table b
on a.user_id=b.user_id""")

configs = [('rateclick_days', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_37_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_37_example_positive_negative_predict_5_27_table12345', configs);


#训练阶段，每个用户每个月的点击频率
sql("""create table wx1_user_last1monthdiffDaysclick_train_table as 
       select user_id,count(distinct visit_datetime) as last1monthrateclick_days
	   from source_data where type=0
	   and visit_datetime>="06-16" and visit_datetime<="07-15"
	   group by user_id""");

sql("""create table wx1_7_6_38_example_positive_negative_train_5_27_table123
as select a.*,b.last1monthrateclick_days
from wx1_7_6_37_example_positive_negative_train_5_27_table12345 a
left outer join wx1_user_last1monthdiffDaysclick_train_table b
on a.user_id=b.user_id""")

configs = [('last1monthrateclick_days', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_38_example_positive_negative_train_5_27_table123', 
'wx1_7_6_38_example_positive_negative_train_5_27_table12345', configs);

sql("""create table wx1_user_last2monthdiffDaysclick_train_table as 
       select user_id,count(distinct visit_datetime) as last2monthrateclick_days
	   from source_data where type=0
	   and visit_datetime>="05-16" and visit_datetime<="06-15"
	   group by user_id""");

sql("""create table wx1_7_6_39_example_positive_negative_train_5_27_table123
as select a.*,b.last2monthrateclick_days
from wx1_7_6_38_example_positive_negative_train_5_27_table12345 a
left outer join wx1_user_last2monthdiffDaysclick_train_table b
on a.user_id=b.user_id""")

configs = [('last2monthrateclick_days', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_39_example_positive_negative_train_5_27_table123', 
'wx1_7_6_39_example_positive_negative_train_5_27_table12345', configs);

sql("""create table wx1_user_last3monthdiffDaysclick_train_table as 
       select user_id,count(distinct visit_datetime) as last3monthrateclick_days
	   from source_data where type=0
	   and visit_datetime>="04-15" and visit_datetime<="05-15"
	   group by user_id""");

sql("""create table wx1_7_6_40_example_positive_negative_train_5_27_table123
as select a.*,b.last3monthrateclick_days
from wx1_7_6_39_example_positive_negative_train_5_27_table12345 a
left outer join wx1_user_last3monthdiffDaysclick_train_table b
on a.user_id=b.user_id""")

configs = [('last3monthrateclick_days', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_40_example_positive_negative_train_5_27_table123', 
'wx1_7_6_40_example_positive_negative_train_5_27_table12345', configs);
	   
#预测阶段,每个用户每个月的点击频率
sql("""create table wx1_user_last1monthdiffDaysclick_predict_table as 
       select user_id,count(distinct visit_datetime) as last1monthrateclick_days
	   from source_data where type=0
	   and visit_datetime>="07-16" and visit_datetime<="08-15"
	   group by user_id""");

sql("""create table wx1_7_6_38_example_positive_negative_predict_5_27_table123
as select a.*,b.last1monthrateclick_days
from wx1_7_6_37_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_user_last1monthdiffDaysclick_predict_table b
on a.user_id=b.user_id""")

configs = [('last1monthrateclick_days', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_38_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_38_example_positive_negative_predict_5_27_table12345', configs);

sql("""create table wx1_user_last2monthdiffDaysclick_predict_table as 
       select user_id,count(distinct visit_datetime) as last2monthrateclick_days
	   from source_data where type=0
	   and visit_datetime>="06-16" and visit_datetime<="07-15"
	   group by user_id""");

sql("""create table wx1_7_6_39_example_positive_negative_predict_5_27_table123
as select a.*,b.last2monthrateclick_days
from wx1_7_6_38_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_user_last2monthdiffDaysclick_predict_table b
on a.user_id=b.user_id""")

configs = [('last2monthrateclick_days', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_39_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_39_example_positive_negative_predict_5_27_table12345', configs);

sql("""create table wx1_user_last3monthdiffDaysclick_predict_table as 
       select user_id,count(distinct visit_datetime) as last3monthrateclick_days
	   from source_data where type=0
	   and visit_datetime>="05-16" and visit_datetime<="06-15"
	   group by user_id""");

sql("""create table wx1_7_6_40_example_positive_negative_predict_5_27_table123
as select a.*,b.last3monthrateclick_days
from wx1_7_6_39_example_positive_negative_predict_5_27_table12345 a
left outer join wx1_user_last3monthdiffDaysclick_predict_table b
on a.user_id=b.user_id""")

configs = [('last3monthrateclick_days', 'null', '0')]
DataProc.fillMissingValues('wx1_7_6_40_example_positive_negative_predict_5_27_table123', 
'wx1_7_6_40_example_positive_negative_predict_5_27_table12345', configs);



#/*****************************抽取样本
sql("""create table  wx10_6_16_3_example_positive_train_5_27_table_brand_top123 as 
       select * from wx1_7_6_40_example_positive_negative_train_5_27_table12345 where buyYN=1""");
sql("""create table  wx10_6_16_3_example_negative_train_5_27_table_brand_top123 as 
       select * from wx1_7_6_40_example_positive_negative_train_5_27_table12345 where buyYN=0""");
print sql("""select count(*) from  wx10_6_16_3_example_positive_train_5_27_table_brand_top123""");
	   
#对表 wx10_6_16_3_example_negative_train_5_27_table_brand_top123随机抽样，抽样1:2,
#由于正样本数为431971,负样本数为908142，抽样表为 wx10_5_27_3_example_negative1_train_5_27_table_brand_top123
i = 431971
DataProc.Sample.randomSample("wx10_6_16_3_example_negative_train_5_27_table_brand_top123",i*5, 
                                     "wx10_6_16_3_example_negative2_train_5_27_table_brand_top123"); 
sql("""create table wx10_6_16_7_train_5_19_5_27_table_data123_quan12345 as 
select * from (
select * from  wx10_6_16_3_example_negative2_train_5_27_table_brand_top123
union all select * from  wx10_6_16_3_example_positive_train_5_27_table_brand_top123)t""");

#/训练表为 wx10_6_16_7_train_5_19_5_27_table_data123_quan12345，训练出 wx10_6_16_4_train_5_27_table_lr_time(删除)，
#预测表不变，仍为wx1_7_6_40_example_positive_negative_predict_5_27_table12345，输入模型 wx10_6_16_4_train_5_27_table_lr_time，
#输出 wx10_6_16_4_predict_5_27_table_lr_time(删除)


rfModel = Classification.RandomForest.train('wx10_6_16_7_train_5_19_5_27_table_data123_quan12345', 
['clickNum1','buyNum1','collectNum1','cartNum1',
'clickNum2','buyNum2','collectNum2','cartNum2',
'clickNum3','buyNum3','collectNum3',"cartNum3",
"diff_date_click_num","diff_date_buy_num",'diff_date_collect_num','diff_date_cart_num',
'click1_noBuy_click','click2_noBuy_click','click3_noBuy_click',
"last1_buynum","last3_buynum","last7_buynum","last10_buynum","last15_buynum",
"last1_collectnum","last3_collectnum","last7_collectnum","last10_collectnum",
"last15_collectnum",
"last1_cartnum","last3_cartnum","last7_cartnum","last10_cartnum",
"last15_cartnum",
"last1_noBuy_click","last3_noBuy_click","last7_noBuy_click","last10_noBuy_click",
"last15_noBuy_click",
"last5_clickNum","last21_clickNum",
"firstOneTen_clickNum","firstSecondTen_clickNum","firstThirdTen_clickNum",
"secondOneTen_clickNum","secondSecondTen_clickNum","secondThirdTen_clickNum",
'firstOneTen_buyNum','firstSecondTen_buyNum','firstThirdTen_buyNum',
'secondOneTen_buyNum','secondSecondTen_buyNum','secondThirdTen_buyNum',
'firstOneTen_noBuy_click','firstSecondTen_noBuy_click','firstThirdTen_noBuy_click',
'secondOneTen_noBuy_click','secondSecondTen_noBuy_click','secondThirdTen_noBuy_click',

'user_last1clickNum','user_last3clickNum','user_last10clickNum',
'user_last1monthbuyNum','user_last2monthbuyNum','user_last3monthbuyNum',
'ratebuy_days',
'last1monthratebuy_days','last2monthratebuy_days','last3monthratebuy_days',
'rateclick_days',
'last1monthrateclick_days','last2monthrateclick_days','last3monthrateclick_days',
'user_last1monthcollectNum','user_last2monthcollectNum','user_last3monthcollectNum',
'user_last1monthcartNum','user_last2monthcartNum','user_last3monthcartNum',

'brand_lastMonthNum','brand_last2MonthNum','brand_last3MonthNum',
'brand_last1DaysNum','brand_last3DaysNum','brand_last7DaysNum',
'brand_last1DaysclickNum','brand_last3DaysclickNum','brand_last7DaysclickNum',

'user_brand_click_buy_pro','brand_click_buy_pro','user_click_buy_pro'], 
[True,True,True,True,True,True,True,True,True,True,
True,True,True,True,True,True,True,True,True,True,
True,True,True,True,True,True,True,True,True,True,
True,True,True,True,True,True,True,True,True,True,
True,True,True,True,True,True,True,True,True,True,
True,True,True,True,True,True,True,True,True,True,
True,True,True,True,True,True,True,True,True,True,
True,True,True,True,True,True,True,True,True,True,
True,True,True,True,True,True,True,True,True,True,
True],
'buyyn', "wx13_6_16_5_train_5_27_table_lr_time", 1000,
maxTreeDeep=95,maxRecordSize =1000000)


Classification.RandomForest.predict("wx1_7_6_40_example_positive_negative_predict_5_27_table12345", rfModel,
"wx13_6_16_5_predict_5_27_table_lr_time",
 appendColNames = ['clickNum1','buyNum1','collectNum1','cartNum1',
'clickNum2','buyNum2','collectNum2','cartNum2',
'clickNum3','buyNum3','collectNum3',"cartNum3",
"diff_date_click_num","diff_date_buy_num",'diff_date_collect_num','diff_date_cart_num',
'click1_noBuy_click','click2_noBuy_click','click3_noBuy_click',
"last1_buynum","last3_buynum","last7_buynum","last10_buynum","last15_buynum",
"last1_collectnum","last3_collectnum","last7_collectnum","last10_collectnum",
"last15_collectnum",
"last1_cartnum","last3_cartnum","last7_cartnum","last10_cartnum",
"last15_cartnum",
"last1_noBuy_click","last3_noBuy_click","last7_noBuy_click","last10_noBuy_click",
"last15_noBuy_click",
"last5_clickNum","last21_clickNum",
"firstOneTen_clickNum","firstSecondTen_clickNum","firstThirdTen_clickNum",
"secondOneTen_clickNum","secondSecondTen_clickNum","secondThirdTen_clickNum",
'firstOneTen_buyNum','firstSecondTen_buyNum','firstThirdTen_buyNum',
'secondOneTen_buyNum','secondSecondTen_buyNum','secondThirdTen_buyNum',
'firstOneTen_noBuy_click','firstSecondTen_noBuy_click','firstThirdTen_noBuy_click',
'secondOneTen_noBuy_click','secondSecondTen_noBuy_click','secondThirdTen_noBuy_click',
'user_last1clickNum','user_last3clickNum','user_last10clickNum',
'user_last1monthbuyNum','user_last2monthbuyNum','user_last3monthbuyNum',
'ratebuy_days',
'last1monthratebuy_days','last2monthratebuy_days','last3monthratebuy_days',
'brand_lastMonthNum','brand_last2MonthNum','brand_last3MonthNum',
'brand_last1DaysNum','brand_last3DaysNum','brand_last7DaysNum',
'brand_last1DaysclickNum','brand_last3DaysclickNum','brand_last7DaysclickNum',
'user_last1monthcollectNum','user_last2monthcollectNum','user_last3monthcollectNum',
'user_last1monthcartNum','user_last2monthcartNum','user_last3monthcartNum',
'user_brand_click_buy_pro','brand_click_buy_pro','user_click_buy_pro',
'rateclick_days','last1monthrateclick_days','last2monthrateclick_days',
'last3monthrateclick_days'], 
isBin = True, labelValueToPredict = '1')


DataProc.appendColumns(['wx1_7_6_40_example_positive_negative_predict_5_27_table12345',
'wx13_6_16_5_predict_5_27_table_lr_time'], 
'wx13_6_16_5_wx13_offer_train_predict_5_27_table_time',selectedColNamesList=[['user_id','brand_id'],['probability']]);
					   
DataProc.sort('wx13_6_16_5_wx13_offer_train_predict_5_27_table_time', 'wx13_6_16_5_wx13_offer_train_predict_5_27_table123_time',
selectedColNames=['probability','user_id'],sortColRule=['-','+']);

DataProc.appendID('wx13_6_16_5_wx13_offer_train_predict_5_27_table123_time', 'wx13_6_16_5_wx13_offer_train_predict_5_27_table123_ID_time', IDColName='ID');

#/*****************************************************调节提交品牌个数*******************************************************************************
sql("""create table  wx13_6_16_5_wx13_offer_top_train_predict_5_27_table123_time as 
select * from  wx13_6_16_5_wx13_offer_train_predict_5_27_table123_ID_time where ID<2710000""");
 
#*****************************提交品牌
sql("drop table t_tmall_add_user_brand_predict_dh");
sql("""create table t_tmall_add_user_brand_predict_dh as
       select 
             user_id,wm_concat(',',brand_id) as brand
       from  wx13_6_16_5_wx13_offer_top_train_predict_5_27_table123_time
       group by user_id""");
	   
#5.68