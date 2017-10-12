drop table if exists fin_certificate_result;

CREATE TABLE `fin_certificate_result` (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='用友财务凭证记录';

CREATE TABLE `fin_vocher` (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='用友财务凭证记录';


DROP PROCEDURE IF EXISTS fin_voucher;  
  delimiter //
  create procedure fin_voucher 
  (IN voucher_type varchar(20), /*凭证类型*/
   IN pstng_date_start varchar(10), /*起始日期*/
   IN pstng_date_end VARCHAR(10) /*结束日期*/
   )
  begin 
  set @voucher_type = voucher_type;
  set @pstng_date_start = pstng_date_start;
  set @vocher_num = 0;

  if pstng_date_end='' 
     then
       set @pstng_date_end = pstng_date_start;
  else 
       set @pstng_date_end = pstng_date_end;
  end if;

  if (upper(@voucher_type) = 'RE') then
      call fin_voucher_re(@pstng_date_start,@pstng_date_end,@re_num);
      select concat_ws(' ','fin_certificate_re','生成数据数据',@re_num);
  elseif (upper(@voucher_type) = 'JISHOU_RE') then
      call fin_voucher_jishou_re(@pstng_date_start,@pstng_date_end,@jishou_num);
      select concat_ws(' ','fin_certificate_jishou_re','生成数据数据',@jishou_num);
  elseif (upper(@voucher_type) = 'RV') then
      call fin_voucher_rv(@pstng_date_start,@pstng_date_end,@rv_num);
      select concat_ws(' ','fin_certificate_rv','生成数据数据',@rv_num);
  elseif (upper(@voucher_type) = 'ZV2') then
      call fin_voucher_zv2(@pstng_date_start,@pstng_date_end,@zv2_num);
      select concat_ws(' ','fin_certificate_zv2','生成数据数据',@zv2_num);
  elseif (upper(@voucher_type) = 'RE_FC') then
      call fin_voucher_re_fc(@pstng_date_start,@pstng_date_end,@re_fc_num);
      select concat_ws(' ','fin_certificate_re_fc','生成数据数据',@re_fc_num);
  elseif (upper(@voucher_type) = 'RV_FC') then
      call fin_voucher_rv_fc(@pstng_date_start,@pstng_date_end,@rv_fc_num);
      select concat_ws(' ','fin_certificate_rv_fc','生成数据数据',@rv_fc_num);
  elseif (upper(@voucher_type) = 'ZV2_CY') then
      call fin_voucher_zv2_cy(@pstng_date_start,@pstng_date_end,@zv2_cy_num);
      select concat_ws(' ','fin_certificate_zv2_cy','生成数据数据',@zv2_cy_num);
  elseif (upper(@voucher_type) = 'ZV2_LS') then
      call fin_voucher_zv2_ls(@pstng_date_start,@pstng_date_end,@zv2_ls_num);
      select concat_ws(' ','fin_certificate_zv2_ls','生成数据数据',@zv2_ls_num);
  elseif (upper(@voucher_type) = 'KA_RV') then
      call fin_voucher_ka_rv(@pstng_date_start,@pstng_date_end,@ka_rv_num);
      select concat_ws(' ','fin_certificate_ka_rv','生成数据数据',@ka_rv_num);    
  elseif (upper(@voucher_type) = 'KA_ZV2') then
      call fin_voucher_ka_zv2(@pstng_date_start,@pstng_date_end,@ka_zv2_num);
      select concat_ws(' ','fin_certificate_ka_zv2','生成数据数据',@ka_zv2_num);     
  elseif (upper(@voucher_type) = 'BSDS_DR') then
      call fin_voucher_bsds_dr(@pstng_date_start,@pstng_date_end,@bsds_dr_num);
      select concat_ws(' ','fin_certificate_bsds_dr','生成数据数据',@bsds_dr_num);
  elseif (upper(@voucher_type) = 'BSDS_RV') then
      call fin_voucher_bsds_rv(@pstng_date_start,@pstng_date_end,@bsds_rv_num);
      select concat_ws(' ','fin_certificate_bsds_rv','生成数据数据',@bsds_rv_num);
  elseif (upper(@voucher_type) = 'BSDS_WE') then
      call fin_voucher_bsds_we(@pstng_date_start,@pstng_date_end,@bsds_we_num);
      select concat_ws(' ','fin_certificate_bsds_we','生成数据数据',@bsds_we_num);
  elseif (upper(@voucher_type) = 'THIRD') then
      call fin_vocher_bank_no_lkl(@pstng_date_start,@pstng_date_end,@third_num);
      select concat_ws(' ','fin_certificate_no_lkl_detail','生成数据数据',@third_num);
  elseif (upper(@voucher_type) = 'CASH') then
      call fin_vocher_bank_lkl(@pstng_date_start,@pstng_date_end,@cash_num);
      select concat_ws(' ','fin_certificate_lkl_detail','生成数据数据',@cash_num);
  elseif (upper(@voucher_type) = 'BANK') then
      call fin_vocher_bank_result(@pstng_date_start,@pstng_date_end,@bank_num);
      select concat_ws(' ','fin_certificate_bank_result','生成数据数据',@bank_num);
  elseif (upper(@voucher_type) = 'THIRD_BUG') then
      call fin_vocher_bank_no_lkl_bug(@pstng_date_start,@pstng_date_end,@third_bug_num);
      select concat_ws(' ','fin_certificate_no_lkl_detail_bug','生成数据数据',@third_bug_num);
  elseif (upper(@voucher_type) = 'CASH_BUG') then
      call fin_vocher_bank_lkl_bug(@pstng_date_start,@pstng_date_end,@cash_bug_num);
      select concat_ws(' ','fin_certificate_lkl_detail_bug','生成数据数据',@cash_bug_num);
  elseif (upper(@voucher_type) = 'BANK_BUG') then
      call fin_vocher_bank_result_bug(@pstng_date_start,@pstng_date_end,@bank_bug_num);
      select concat_ws(' ','fin_certificate_bank_result_bug','生成数据数据',@bank_bug_num);
  elseif (upper(@voucher_type) = 'KZ') then
      call fin_voucher_rece(@pstng_date_start,@pstng_date_end,@kz_num);
      select concat_ws(' ','fin_certificate_rece','生成数据数据',@kz_num);
  elseif (upper(@voucher_type) = 'ALL') then
      call fin_voucher_result(@all_num);
      select concat_ws(' ','fin_voucher','生成数据数据',@all_num);
  else 
      select '凭证参数传入有误';
  end if;

end
 //
 
# call fin_voucher('bsds_dr','2017-07-01','2017-07-10');



