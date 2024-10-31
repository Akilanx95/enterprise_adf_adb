# Databricks notebook source
# DBTITLE 1,ALTER & OPTIMZE FOR ausp
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.ausp CLUSTER BY  (objek, atinn, atzhl);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.ausp

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR cabn
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.cabn CLUSTER BY  (atinn);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.cabn

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR cawnt
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.cawnt CLUSTER BY  (atinn, atzhl);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.cawnt

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR cdhdr
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.cdhdr CLUSTER BY  (objectclas, objectid, changenr);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.cdhdr

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR cdpos
# MAGIC %sql
# MAGIC -- explain ALTER TABLE cotybrcatalog_prd.silver_sap_hana.cdpos CLUSTER BY  ( objectid, changenr, tabkey, fname);
# MAGIC -- explain OPTIMIZE cotybrcatalog_prd.silver_sap_hana.cdpos

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR dd07l
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.dd07l CLUSTER BY  (domname, valpos);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.dd07l

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR j_1bnfdoc
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.j_1bnfdoc CLUSTER BY  (docnum);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.j_1bnfdoc

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR j_1bnflin
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.j_1bnflin CLUSTER BY  (docnum, itmnum);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.j_1bnflin

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR kna1
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.kna1 CLUSTER BY  (kunnr);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.kna1

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR knb1
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.knb1 CLUSTER BY  (kunnr);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.knb1

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR knvv
# MAGIC %sql
# MAGIC ALTER TABLE cotybrcatalog_prd.silver_sap_hana.knvv CLUSTER BY  (kunnr);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.knvv

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR konv
# MAGIC %sql
# MAGIC -- explain ALTER TABLE cotybrcatalog_prd.silver_sap_hana.konv CLUSTER BY  (knumv, kposn, stunr);
# MAGIC -- explain OPTIMIZE cotybrcatalog_prd.silver_sap_hana.konv

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR lips
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.lips CLUSTER BY  (vbeln, posnr);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.lips

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR makt
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.makt CLUSTER BY  (matnr);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.makt

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR mara
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.mara CLUSTER BY  (matnr);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.mara

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR marm
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.marm CLUSTER BY  (matnr, meinh);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.marm

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR mvke
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.mvke CLUSTER BY  (matnr);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.mvke

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR pa0002
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.pa0002 CLUSTER BY  (pernr, begda);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.pa0002

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR t001
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.t001 CLUSTER BY  (bukrs);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.t001

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR t001k
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.t001k CLUSTER BY  (bwkey);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.t001k

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR t001w
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.t001w CLUSTER BY  (werks);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.t001w

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR t006a
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.t006a CLUSTER BY  (msehi);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.t006a

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR t171
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.t171 CLUSTER BY  (bzirk);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.t171

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR t171t
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.t171t CLUSTER BY  (bzirk);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.t171t

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR t179t
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.t179t CLUSTER BY  (prodh);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.t179t

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR tcurr
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.tcurr CLUSTER BY  (fcurr, tcurr, gdatu);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.tcurr

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR tspat
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.tspat CLUSTER BY  (spart);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.tspat

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR tvagt
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.tvagt CLUSTER BY  (abgru);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.tvagt

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR tvgrt
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.tvgrt CLUSTER BY  (vkgrp);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.tvgrt

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR tvkbt
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.tvkbt CLUSTER BY  (vkbur);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.tvkbt

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR tvm3t
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.tvm3t CLUSTER BY  (mvgr3);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.tvm3t

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR tvm4t
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.tvm4t CLUSTER BY  (mvgr4);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.tvm4t

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR tvtkt
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.tvtkt CLUSTER BY  (shtyp);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.tvtkt

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR tvtw
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.tvtw CLUSTER BY  (vtweg);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.tvtw

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR tvtwt
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.tvtwt CLUSTER BY  (vtweg);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.tvtwt

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR vbak
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.vbak CLUSTER BY  (vbeln);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.vbak

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR vbap
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.vbap CLUSTER BY  (vbeln, posnr);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.vbap

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR vbfa
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.vbfa CLUSTER BY  (vbelv, posnv, vbeln, posnn);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.vbfa

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR vbfs
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.vbfs CLUSTER BY  (sammg, vbeln, posnr, etenr);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.vbfs

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR vbkd
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.vbkd CLUSTER BY  (vbeln, posnr);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.vbkd

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR vbpa
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.vbpa CLUSTER BY  (vbeln, posnr);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.vbpa

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR vbrk
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.vbrk CLUSTER BY  (vbeln);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.vbrk

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR vbrp
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.vbrp CLUSTER BY  (vbeln, posnr);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.vbrp

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR vttk
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.vttk CLUSTER BY  (tknum);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.vttk

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR vttp
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.vttp CLUSTER BY  (tknum, tpnum);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.vttp

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR vtts
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.vtts CLUSTER BY  (tknum, tsnum);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.vtts

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR zbwsdt01
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.zbwsdt01 CLUSTER BY  (auart, pstyv);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.zbwsdt01

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR zsdt044
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.zsdt044 CLUSTER BY  (versao, matnr, matkl, kondm);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.zsdt044

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR zsdt072
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.zsdt072 CLUSTER BY  (versao);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.zsdt072

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR zsdt501
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.zsdt501 CLUSTER BY  (vkbur);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.zsdt501

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR zsdt502
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.zsdt502 CLUSTER BY  (vkgrp, vkbur, zdiretor);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.zsdt502

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR zsdt505
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.zsdt505 CLUSTER BY  (bzirk, vkbur, vkgrp);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.zsdt505

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR zsdtedi010
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.zsdtedi010 CLUSTER BY  (zedi_chavrg);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.zsdtedi010

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR zsdtedi011
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.zsdtedi011 CLUSTER BY  (zedi_chavrg, zedi_itmseq);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.zsdtedi011

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR zsdtedi031
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.zsdtedi031 CLUSTER BY  (zedi_cod_mrecusa, zedi_motivo_recusa);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.zsdtedi031

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR ztrt003
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.ztrt003 CLUSTER BY  (ocorrencia, item, tknum);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.ztrt003

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR ztrt007
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.ztrt007 CLUSTER BY  (docnum);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.ztrt007

# COMMAND ----------

# DBTITLE 1,ALTER & OPTIMZE FOR ztrt007d
# MAGIC %sql
# MAGIC -- ALTER TABLE cotybrcatalog_prd.silver_sap_hana.ztrt007d CLUSTER BY  (docnum);
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.ztrt007d

# COMMAND ----------

# DBTITLE 1,OPTIMZE FOR de_para_recusa
# MAGIC %sql
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.de_para_recusa

# COMMAND ----------

# DBTITLE 1,OPTIMZE FOR t141t
# MAGIC %sql
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.t141t

# COMMAND ----------

# DBTITLE 1,OPTIMZE FOR zsdt103
# MAGIC %sql
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.zsdt103

# COMMAND ----------

# DBTITLE 1,OPTIMZE FOR zsdt506
# MAGIC %sql
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.zsdt506

# COMMAND ----------

# DBTITLE 1,OPTIMZE FOR zsdt507
# MAGIC %sql
# MAGIC OPTIMIZE cotybrcatalog_prd.silver_sap_hana.zsdt507
