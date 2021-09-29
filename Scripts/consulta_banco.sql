-- 1 – Número de casos de COVID-19 por região administrativa do estado;
-- 2 – Número de óbitos decorrentes da COVID-19 por região administrativa do estado
select
regiao_administrativa.codigo, 
regiao_administrativa.nome,
count(distinct municipio.ibge) municipios,
sum(case when obito then 1 else 0 end) obitos,
count(1) casos
from caso
inner join paciente on paciente.codigo = caso.paciente
inner join municipio on municipio.ibge = paciente.municipio
inner join regiao_administrativa on regiao_administrativa.codigo = municipio.regiao_administrativa 
group by regiao_administrativa.codigo, regiao_administrativa.nome

-- 3 – Relação entre comorbidades e número de casos e óbitos no estado por gênero;
select 
comorbidade_codigo, 
comorbidade_descricao, 
genero,
sum(case when obito then 1 else 0 end) obitos,
count(1) casos,
cast(( cast(sum(case when obito then 1 else 0 end) * 100 as numeric(38,2)) / cast(count(1) as numeric(38,2))) as numeric(5,2)) porcentagem
from (
	select 
	comorbidade.codigo comorbidade_codigo, 
	comorbidade.descricao comorbidade_descricao,
	paciente.genero genero,
	obito 
	from caso
	inner join paciente on paciente.codigo = caso.paciente
	left join comorbidade_paciente on paciente.codigo = comorbidade_paciente.paciente 
	left join comorbidade on comorbidade.codigo = comorbidade_paciente.comorbidade
	where paciente.genero = 'MASCULINO'
	union all 
	select 
	comorbidade.codigo, 
	comorbidade.descricao,
	paciente.genero,
	obito 
	from caso
	inner join paciente on paciente.codigo = caso.paciente
	left join comorbidade_paciente on paciente.codigo = comorbidade_paciente.paciente 
	left join comorbidade on comorbidade.codigo = comorbidade_paciente.comorbidade
	where paciente.genero = 'FEMININO'
) casos_comorbidades_generos
group by comorbidade_codigo, comorbidade_descricao, genero 
order by comorbidade_descricao 

-- 4 - Relação entre idade e número de caos e óbitos no estado por gênero;
select 
(case when paciente.idade <= 18 then
'0 a 18'
when paciente.idade <= 23 then
'18 a 23'
when paciente.idade <= 28 then
'24 a 28'
when paciente.idade <= 33 then
'29 a 33'
when paciente.idade <= 38 then
'33 a 38'
when paciente.idade <= 43 then
'39 a 43'
when paciente.idade <= 48 then
'44 a 48'
when paciente.idade <= 53 then
'49 a 53'
when paciente.idade <= 58 then
'54 a 58'
else 
'+ de 59 anos'
end) idade,
paciente.genero,
sum(case when obito then 1 else 0 end) obitos,
count(1)
from caso
inner join paciente on caso.paciente = paciente.codigo 
where paciente.genero = 'MASCULINO' or paciente.genero = 'FEMININO'
group by 1, paciente.genero 

-- Casos e óbitos por município, utilizado para apresentar o mapa
select
municipio.ibge, 
municipio.nome,
municipio.latitude, 
municipio.longitude,
sum(case when obito then 1 else 0 end) obitos,
count(1) casos
from caso
inner join paciente on paciente.codigo = caso.paciente
inner join municipio on municipio.ibge = paciente.municipio
group by municipio.ibge, municipio.nome, municipio.latitude, municipio.longitude 

-- Taxa de isolamento e número de casos por dia
select
regiao_administrativa.codigo, 
regiao_administrativa.nome,
data_inicio_sintomas,
isolamento.data_isolamento,
sum(case when obito then 1 else 0 end) obitos,
count(1) casos,
sum(isolamento.media_isolamento * municipio.populacao) / sum(municipio.populacao) as taxa_isolamento
from caso
inner join paciente on paciente.codigo = caso.paciente
inner join municipio on municipio.ibge = paciente.municipio
inner join regiao_administrativa on regiao_administrativa.codigo = municipio.regiao_administrativa 
right join isolamento on isolamento.municipio = municipio.ibge and caso.data_inicio_sintomas = isolamento.data_isolamento
where data_inicio_sintomas is not null
group by regiao_administrativa.codigo, regiao_administrativa.nome, caso.data_inicio_sintomas,isolamento.data_isolamento
order by caso.data_inicio_sintomas