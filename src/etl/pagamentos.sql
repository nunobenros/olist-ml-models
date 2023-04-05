-- Databricks notebook source
WITH tb_join AS (

SELECT 
  pgmt.*,
  itm.idVendedor  
FROM silver.olist.pedido as ped
left join
  silver.olist.pagamento_pedido  as pgmt
  on ped.idPedido=pgmt.idPedido  
LEFT JOIN 
  silver.olist.item_pedido as itm
ON ped.idPedido= itm.idPedido
where ped.dtPedido <'2018-01-01' and ped.dtPedido >= ADD_MONTHS('2018-01-01',-6)
and itm.idVendedor is not null 
),
tb_group as (
SELECT idVendedor,
  descTipoPagamento,
  --count( descTipoPagamento) as QTD_por_pgmt
  count(distinct idPedido) as qtdPedido,
  sum(vlPagamento) as valor
FROM tb_join
group by 
idVendedor,
descTipoPagamento
order by idVendedor
)

select idVendedor,
sum(case when descTipoPagamento =  'boleto'      then  qtdPedido else 0 end) as qtd_boleto,  
sum(case when descTipoPagamento =  'credit_card' then  qtdPedido else 0 end) as qtd_credit_card,
sum(case when descTipoPagamento =  'voucher'     then  qtdPedido else 0 end) as qtd_voucher,
sum(case when descTipoPagamento =  'debit_card'  then  qtdPedido else 0 end) as qtd_debit_card,

sum(case when descTipoPagamento =  'boleto'      then  qtdPedido else 0 end)/ sum(qtdPedido)*100 as  pct_qtd_boleto,  
sum(case when descTipoPagamento =  'credit_card' then  qtdPedido else 0 end)/ sum(qtdPedido)*100 as  pct_qtd_credit_card,
sum(case when descTipoPagamento =  'voucher'     then  qtdPedido else 0 end)/ sum(qtdPedido)*100 as  pct_qtd_voucher,
sum(case when descTipoPagamento =  'debit_card'  then  qtdPedido else 0 end)/ sum(qtdPedido)*100 as  pct_qtd_debit_card,


sum(case when descTipoPagamento =  'boleto'      then  valor else 0 end) as valor_boleto,  
sum(case when descTipoPagamento =  'credit_card' then  valor else 0 end) as valor_credit_card,
sum(case when descTipoPagamento =  'voucher'     then  valor else 0 end) as valor_voucher,
sum(case when descTipoPagamento =  'debit_card'  then  valor else 0 end) as valor_debit_card,

sum(case when descTipoPagamento =  'boleto'      then  valor else 0 end)/sum(valor)*100 as pct_valor_boleto,  
sum(case when descTipoPagamento =  'credit_card' then  valor else 0 end)/sum(valor)*100 as pct_valor_credit_card,
sum(case when descTipoPagamento =  'voucher'     then  valor else 0 end)/sum(valor)*100 as pct_valor_voucher,
sum(case when descTipoPagamento =  'debit_card'  then  valor else 0 end)/sum(valor)*100 as pct_valor_debit_card
from tb_group

group by 1 

-- COMMAND ----------

SELECT 
  *
FROM silver.olist.pagamento_pedido

--where dtPedido <'2018-01-01' and dtPedido >= ADD_MONTHS('2018-01-01',-6)
