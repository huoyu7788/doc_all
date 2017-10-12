drop table if exists fin_voucher;
CREATE TABLE `fin_voucher` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(10) DEFAULT NULL COMMENT '地区',
  `receipt_id` varchar(32) DEFAULT NULL COMMENT '签收单',
  `tax_rate` char(5) DEFAULT NULL COMMENT '税率',
  `dctype` varchar(10) DEFAULT NULL COMMENT '凭证类型',
  `cd_type` varchar(10) DEFAULT NULL COMMENT '借贷标志',
  `doc_number` varchar(32) DEFAULT NULL COMMENT '科目编码',
  `dd` varchar(10) DEFAULT NULL COMMENT '部门编码',
  `project` char(10) DEFAULT NULL COMMENT '项目大类',
  `project_cate` char(10) DEFAULT NULL COMMENT '项目',
  `money` decimal(16,2) DEFAULT NULL COMMENT '金额',
  `text` varchar(128) DEFAULT NULL COMMENT '文本',
  `pstng_date` varchar(16) DEFAULT NULL COMMENT '时间',
  `customer` varchar(32) DEFAULT NULL COMMENT '承运商编码',
  `vendor` varchar(32) DEFAULT NULL COMMENT '供应商编码',
  `status` int(10) unsigned NOT NULL DEFAULT '1' COMMENT '1-新建，2-已导入，3-导入失败',
  `created_at` int(11) DEFAULT NULL COMMENT '创建时间',
  `updated_at` int(11) DEFAULT NULL COMMENT '修改时间',
  PRIMARY KEY (`id`)  
) ENGINE=InnoDB CHARSET=utf8 COMMENT='用友财务凭证记录';



DROP PROCEDURE IF EXISTS fin_voucher_result;  
  delimiter //
  create procedure fin_voucher_result (out result_num int)
  begin 
  set @strsql = '
        insert into fin_voucher
               (name,receipt_id,tax_rate,dctype,cd_type,doc_number,dd,project,project_cate,
               money,text,pstng_date,customer,vendor,status,created_at,updated_at)
        select name,receipt_id,tax_rate,dctype,cd_type,doc_number,dd,project,project_cate,
               money,text,pstng_date,customer,vendor,status,created_at,updated_at
          from fin_certificate_result';

  PREPARE slesql FROM @strsql; 
  EXECUTE slesql ; 
  DROP PREPARE slesql;

  select FOUND_ROWS() into result_num; 
end
//

# call fin_vocher_result(@result_num);

