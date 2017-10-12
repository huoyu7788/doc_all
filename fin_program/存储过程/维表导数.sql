CREATE TABLE `dim_fin_certificate_info_u8` (
  `name` varchar(30) DEFAULT NULL COMMENT '地区',
  `doc_type` varchar(10) DEFAULT NULL COMMENT '凭证类型',
  `dctype` varchar(10) DEFAULT NULL COMMENT '借贷标志',
  `gl_account` varchar(10) DEFAULT NULL COMMENT '科目编码',
  `dd_name` varchar(30) DEFAULT NULL COMMENT '部门名称',
  `dd` varchar(10) DEFAULT NULL COMMENT '部门号',
  `customer` varchar(10) DEFAULT NULL COMMENT '承运商编码',
  `project` varchar(10) DEFAULT NULL COMMENT '项目大类',
  `project_cate` varchar(10) DEFAULT NULL COMMENT '项目',
  `pn` int(11) DEFAULT '1' COMMENT '正负标志',
  `alloc_nmbr` varchar(10) DEFAULT NULL,
  `tax_rate` varchar(10) DEFAULT NULL COMMENT '税率',
  `amtdoccur` varchar(50) DEFAULT NULL,
  `item_text` varchar(50) DEFAULT NULL COMMENT '文本信息',
  `system` varchar(20) DEFAULT NULL COMMENT '系统',
  `area` varchar(20) DEFAULT NULL COMMENT '地区',
  `flag` varchar(20) DEFAULT NULL COMMENT '凭证标示'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

mysql -h192.168.70.7 -uvrmdev -pPassvrmdev2017 -P3307 --local-infile=1  -e "
SET character_set_database = utf8;
SET character_set_server = utf8;
LOAD DATA LOCAL INFILE '/home/inf/zhaoning/u8/dimension/dim_fin_certificate_info_u8_new.sql' REPLACE INTO TABLE lsh_vrm.dim_fin_certificate_info_u8 (name, doc_type, dctype, gl_account, dd_name, dd, customer, project, project_cate, pn, alloc_nmbr, tax_rate, amtdoccur, item_text, system, area, flag )
"


drop table if exists dim_third_pay_u8;
CREATE TABLE `dim_third_pay_u8` (
  `name` varchar(30) DEFAULT NULL COMMENT '地区',
  `doc_type` varchar(10) DEFAULT NULL COMMENT '凭证类型',
  `dctype` varchar(10) DEFAULT NULL COMMENT '借贷标志',
  `gl_account` varchar(10) DEFAULT NULL COMMENT '科目编码',
  `dd_name` varchar(50) DEFAULT NULL COMMENT '部门名称',
  `dd` varchar(10) DEFAULT NULL COMMENT '部门号',
  `customer` varchar(10) DEFAULT NULL COMMENT '承运商编码',
  `project` varchar(10) DEFAULT NULL COMMENT '项目大类',
  `project_cate` varchar(10) DEFAULT NULL COMMENT '项目',
  `pn` int(11) DEFAULT '1' COMMENT '正负标志',
  `alloc_nmbr` varchar(10) DEFAULT NULL,
  `tax_rate` varchar(10) DEFAULT NULL COMMENT '税率',
  `amtdoccur` varchar(50) DEFAULT NULL,
  `item_text` varchar(50) DEFAULT NULL COMMENT '文本信息',
  `system` varchar(20) DEFAULT NULL COMMENT '系统',
  `area` varchar(20) DEFAULT NULL COMMENT '地区',
  `flag` varchar(50) DEFAULT NULL COMMENT '凭证标示'
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

mysql -h192.168.70.7 -uvrmdev -pPassvrmdev2017 -P3307 --local-infile=1  -e "
SET character_set_database = utf8;
SET character_set_server = utf8;
LOAD DATA LOCAL INFILE '/home/inf/zhaoning/u8/dimension/mds_fin_u8_third_pay_dimension.sql' REPLACE INTO TABLE lsh_vrm.dim_third_pay_u8 (name, doc_type, dctype, gl_account, dd_name, dd, customer, project, project_cate, pn, alloc_nmbr, tax_rate, amtdoccur, item_text, system, area, flag )
"