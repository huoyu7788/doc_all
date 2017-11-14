drop table mall_bsds_sale_detail;
CREATE TABLE `mall_bsds_sale_detail` (
  `id` bigint(20),
  `zone_id` varchar(32) NOT NULL DEFAULT '1000' COMMENT '区域id: 1000-北京,1001-天津, 1002-杭州',
  `zone_name` varchar(32) NOT NULL DEFAULT '北京' COMMENT '区域名: 北京,天津,杭州',
  `sale_id` bigint(20) unsigned NOT NULL COMMENT '销售id',
  `product_code` varchar(32) DEFAULT '' COMMENT '物美编码',
  `product_name` varchar(128) DEFAULT '' COMMENT '商品名称',
  `qty` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '销售数量',
  `real_qty` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '销售数量EA',
  `price` decimal(15,4) NOT NULL COMMENT '原售价',
  `average_price` decimal(15,4) NOT NULL COMMENT '移动平均价 取发货单日期价格or签收单日期价格',
  `nt_price` decimal(15,4) NOT NULL COMMENT '未税进货价 ??',
  `tax` tinyint(3) unsigned NOT NULL COMMENT '税率 17/13/11/0',
  `type` tinyint(3) unsigned NOT NULL DEFAULT '0' COMMENT '商品模式 0-物美，1-寄售,2-代销',
  `fin_code` varchar(32) NOT NULL COMMENT '商品所属供商账务编码',
  `warehouse_id` varchar(64) NOT NULL DEFAULT '' COMMENT '仓库ID',
  `day` varchar(16) NOT NULL COMMENT '数据同步时间，精确到天',
  `created_at` int(14) unsigned NOT NULL DEFAULT '0',
  `updated_at` int(14) unsigned NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



truncate table mds_fin_sale_detail;
truncate table fin_certificate_result;
insert into mds_fin_sale_detail 
select zone_id,zone_name,sale_id,0 as pay_type,0 as final_amount,0 as afs_amount,0 as user_type,
       0 customer,product_code,product_name,price,qty,real_qty,tax,average_price,nt_price,
       type,fin_code as vendor,'' as user_fin_code,day as pstng_date,'' as provider
  from mall_bsds_sale_detail 


mysql -h192.168.70.7 -uvrmdev -pPass2017vrmdev -P3307 --local-infile=1  -e "
SET character_set_database = utf8;
SET character_set_server = utf8;
LOAD DATA LOCAL INFILE '/home/inf/zhaoning/u8/data/201710/bsds_78910.txt' INTO TABLE lsh_vrm.mall_bsds_sale_detail(zone_id, zone_name, sale_id, product_code, product_name, qty, real_qty, price, average_price, nt_price, tax, type, fin_code, warehouse_id, day)
"

















    
