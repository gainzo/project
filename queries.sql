-- J'utilise ici la syntaxe BigQuery SQL.
-- Ce code peut-être adapté pour fonctionner avec d'autres moteurs, en modifiant notamment les types date pour ne pas compter sur les conversions tacites, ou encore en adaptant le pivot

-- Première partie
select
    format_date("%d/%m/%Y", parse_date("%d/%m/%y", date)) as date,
    sum(prod_price*prod_qty) as ventes
from TRANSACTION
where parse_date("%d/%m/%y", date) between "2019-01-01" and "2019-12-31"
group by date
order by date


-- Seconde partie
with
    ventes as (
        select
            client_id,
            prod_id,
            prod_price*prod_qty as vente
        from TRANSACTION
        where parse_date("%d/%m/%y", date) between "2020-01-01" and "2020-12-31"
    ),

    ventes_meuble_deco as (
        select
            a.client_id,
            b.product_type,
            a.vente
        from ventes a
        inner join PRODUCT_NOMENCLATURE b
            on a.prod_id = b.product_id
        where b.product_type in ("MEUBLE", "DECO")
    ),

    ventes_meuble_deco_agg as (
        select
            *
        from ventes_meuble_deco pivot(SUM(vente) FOR product_type IN ('MEUBLE', 'DECO'))
    )

select
    client_id,
    MEUBLE as ventes_meuble,
    DECO as ventes_deco
from ventes_meuble_deco_agg