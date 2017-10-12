select* from GL_accvouch where ino_id='43' and iperiod='8'
delete from GL_accvouch where ino_id='45' and iperiod='8'



SELECT TOP (20000000) i_id, iperiod, csign, isignseq, ino_id, inid,dbill_date,
idoc, cbill,  ccheck, cbook, ibook, ccashier, iflag, ctext1, ctext2, cdigest, ccode, 
cexch_name, md, mc, md_f, mc_f, nfrat, nd_s, nc_s, csettle, cn_id, dt_date, cdept_id, 
cperson_id, ccus_id, csup_id, citem_id, citem_class, cname, ccode_equal, iflagbank, 
iflagPerson, bdelete, coutaccset, ioutyear, coutsysname, coutsysver, doutbilldate, 
ioutperiod, coutsign, coutno_id, doutdate, coutbillsign, coutid, bvouchedit, bvouchAddordele, 
bvouchmoneyhold, bvalueedit, bcodeedit, ccodecontrol, bPCSedit, bDeptedit, bItemedit, 
bCusSupInput, cDefine1, cDefine2, cDefine3, cDefine4, cDefine5, cDefine6, cDefine7, 
cDefine8, cDefine9, cDefine10, cDefine11, cDefine12, cDefine13, cDefine14, cDefine15, 
cDefine16, dReceive, cWLDZFlag, dWLDZTime, bFlagOut, iBG_OverFlag, cBG_Auditor, 
dBG_AuditTime, cBG_AuditOpinion, bWH_BgFlag, ssxznum, CErrReason, BG_AuditRemark, 
cBudgetBuffer, iBG_ControlResult, NCVouchID, daudit_date, RowGuid, cBankReconNo, 
iyear, iYPeriod, wllqDate, wllqPeriod, tvouchtime, cblueoutno_id, ccodeexch_equal
FROM GL_accvouch
WHERE (iperiod = 8) AND (ccode LIKE '6401%')