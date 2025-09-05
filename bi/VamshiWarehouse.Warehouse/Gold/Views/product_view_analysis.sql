-- Auto Generated (Do not modify) 9C41A797B84F8E5D16BE385DB7D658D55B7495DC44D75CAA32B812E32C2F8B09
CREATE view Gold.product_view_analysis AS
with cte_1 AS
(
select  
F.*,
P.ModelName,
P.ProductName,
P.ProductPrice
 from VamshiWarehouse.Gold.Fact_sales F
 left join VamshiWarehouse.Gold.dim_products P
 on F.ProductKey=P.ProductKey
 )
 select cte_1.ModelName,
 COUNT(DISTINCT(cte_1.CustomerKey)) As Total_customercount,
 COUNT(DISTINCT(cte_1.TerritoryKey)) As Total_Territorycount
 from 
 cte_1
 GROUP BY
 cte_1.ModelName