#Para rodar esse arquivo, é necessário criar a base de dados com as tabelas utilizando os scripts disponíveis em Script/create_database.sql

#caso ainda nao tenha os pacotes instalados

install.packages("DT") 
install.packages("knitr")
install.packages("formattable")
install.packages("flextable")
install.packages("gt")
install.packages("patchwork")
install.packages("RPostgreSQL")
install.packages("lubridate")
install.packages("geobr")
install.packages("ggspatial")
install.packages("crul")


library("RPostgreSQL")
library(tidyverse)
library(dplyr)
library(DT)
library(knitr)
library(formattable)
library(flextable)
library(gt)
library(patchwork)
library(lubridate)
library(geobr)
library(ggspatial)
library(crul)
library(evaluate)
library(plotly)


#Alterar variáveis de acordo com as configurações do computador
diretorio_arquivos_csv <- "C:/Users/Gustavo/OneDrive/ECOLOGIA/Pós-graduação/Especialização/Módulo 1/Projeto Integrador"
host_banco <- "localhost"
porta_banco <- 5432
usuario_banco <- "postgres"
senha_banco <- "12345"
nome_database <- "PI_video"

#realiza conexão com o banco de dados PostgreSQL
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, 
                 dbname = nome_database,
                 host = host_banco, 
                 port = porta_banco,
                 user = usuario_banco, 
                 password = senha_banco
                 )
# remove a senha
rm (senha_banco)

#Inicia leitura e tratamento de dados para inclusão no banco de dados

#REGIÕES ADMINISTRATIVAS E MUNICÍPIOS
df_municipios <- read_csv2(
  paste0(diretorio_arquivos_csv, "20210510_dados_covid_municipios_sp.csv"),
  locale = locale(encoding = "UTF-8")
)

#Prepara e insere os dados da tabela de regiões administrativas
df_tabela_regioes_administrativas <- 
  distinct(df_municipios, 
           codigo = cod_ra, 
           nome = nome_ra) %>% 
  filter(!is.na(nome))
dbWriteTable(con, "regiao_administrativa", df_tabela_regioes_administrativas, row.names=FALSE, append=TRUE)

#Prepara e insere os dados da tabela de municípios
df_tabela_municipios <- 
  distinct(df_municipios, 
           ibge = codigo_ibge, 
           nome = toupper(nome_munic), 
           regiao_administrativa = ifelse(cod_ra == 0, NA, cod_ra), 
           populacao = pop,
           latitude = latitude,
           longitude = longitude)
dbWriteTable(con, "municipio", df_tabela_municipios, row.names=FALSE, append=TRUE)

#PACIENTES, COMORBIDADES E casos 
df_micro_dados_casos <- read_csv2(
  paste0(diretorio_arquivos_csv, "20210510_Casos-e-obitos-ESP.csv"),
  locale = locale(encoding = "UTF-8")
)

#Prepara e insere os dados da tabela de comorbidades
descricoes_comorbidades = names(df_micro_dados_casos[3:15])
codigos_comorbidades = seq(1,length(descricoes_comorbidades))
df_tabela_comorbidades = data.frame(codigo=codigos_comorbidades, descricao=descricoes_comorbidades)
dbWriteTable(con, "comorbidade", df_tabela_comorbidades, row.names=FALSE, append=TRUE)

#Prepara e insere os dados da tabela de pacientes
df_micro_dados_casos$codigo_paciente <- seq(1, nrow(df_micro_dados_casos))
#Ajusta dados de municípios informados de forma divergente
df_micro_dados_casos$Municipio[df_micro_dados_casos$Municipio == "ARCO ÍRIS"] <- "ARCO-ÍRIS"
df_micro_dados_casos$Municipio[df_micro_dados_casos$Municipio == "BIRITIBA-MIRIM"] <- "BIRITIBA MIRIM"
df_micro_dados_casos$Municipio[df_micro_dados_casos$Municipio == "ITAÓCA"] <- "ITAOCA"
df_merge_municipio <- left_join(df_micro_dados_casos, df_tabela_municipios, by = c("Municipio" = "nome"))
df_tabela_pacientes <- select(df_merge_municipio, codigo=codigo_paciente, genero=Genero, idade=Idade, municipio=ibge)
dbWriteTable(con, "paciente", df_tabela_pacientes, row.names=FALSE, append=TRUE)

#Prepara e insere os relacionamentos entre pacientes e comorbidades
#Asma
df_micro_dados_casos_filter <- filter(df_micro_dados_casos, Asma=="SIM")
if (nrow(df_micro_dados_casos_filter) > 0) {
  df_tabela_comorbidades_paciente <- 
    data.frame(
      select(df_micro_dados_casos_filter, 
             codigo_paciente), 
      select(filter(df_tabela_comorbidades, descricao=="Asma"), 
             codigo)
    )
  df_tabela_comorbidades_paciente <- rename(df_tabela_comorbidades_paciente, comorbidade=codigo)
  df_tabela_comorbidades_paciente <- rename(df_tabela_comorbidades_paciente, paciente=codigo_paciente)
  dbWriteTable(con, "comorbidade_paciente", df_tabela_comorbidades_paciente, row.names=FALSE, append=TRUE)
}
#Diabetes
df_micro_dados_casos_filter <- filter(df_micro_dados_casos, Diabetes=="SIM")
if (nrow(df_micro_dados_casos_filter) > 0) {
  df_tabela_comorbidades_paciente <- 
    data.frame(
      select(df_micro_dados_casos_filter, 
             codigo_paciente), 
      select(filter(df_tabela_comorbidades, descricao=="Diabetes"), 
             codigo)
    )
  df_tabela_comorbidades_paciente <- rename(df_tabela_comorbidades_paciente, comorbidade=codigo)
  df_tabela_comorbidades_paciente <- rename(df_tabela_comorbidades_paciente, paciente=codigo_paciente)
  dbWriteTable(con, "comorbidade_paciente", df_tabela_comorbidades_paciente, row.names=FALSE, append=TRUE)
}
#Cardiopatia
df_micro_dados_casos_filter <- filter(df_micro_dados_casos, Cardiopatia=="SIM")
if (nrow(df_micro_dados_casos_filter) > 0) {
  df_tabela_comorbidades_paciente <- 
    data.frame(
      select(df_micro_dados_casos_filter, 
             codigo_paciente), 
      select(filter(df_tabela_comorbidades, descricao=="Cardiopatia"), 
             codigo)
    )
  df_tabela_comorbidades_paciente <- rename(df_tabela_comorbidades_paciente, comorbidade=codigo)
  df_tabela_comorbidades_paciente <- rename(df_tabela_comorbidades_paciente, paciente=codigo_paciente)
  dbWriteTable(con, "comorbidade_paciente", df_tabela_comorbidades_paciente, row.names=FALSE, append=TRUE)
}
#Doenca Hematologica
df_micro_dados_casos_filter <- filter(df_micro_dados_casos, df_micro_dados_casos["Doenca Hematologica"]=="SIM")
if (nrow(df_micro_dados_casos_filter) > 0) {
  df_tabela_comorbidades_paciente <- 
    data.frame(
      select(df_micro_dados_casos_filter, 
             codigo_paciente), 
      select(filter(df_tabela_comorbidades, descricao=="Doenca Hematologica"), 
             codigo)
    )
  df_tabela_comorbidades_paciente <- rename(df_tabela_comorbidades_paciente, comorbidade=codigo)
  df_tabela_comorbidades_paciente <- rename(df_tabela_comorbidades_paciente, paciente=codigo_paciente)
  dbWriteTable(con, "comorbidade_paciente", df_tabela_comorbidades_paciente, row.names=FALSE, append=TRUE)
}
#Doenca Hepatica
df_micro_dados_casos_filter <- filter(df_micro_dados_casos, df_micro_dados_casos["Doenca Hepatica"]=="SIM")
if (nrow(df_micro_dados_casos_filter) > 0) {
  df_tabela_comorbidades_paciente <- 
    data.frame(
      select(df_micro_dados_casos_filter, 
             codigo_paciente), 
      select(filter(df_tabela_comorbidades, descricao=="Doenca Hepatica"), 
             codigo)
    )
  df_tabela_comorbidades_paciente <- rename(df_tabela_comorbidades_paciente, comorbidade=codigo)
  df_tabela_comorbidades_paciente <- rename(df_tabela_comorbidades_paciente, paciente=codigo_paciente)
  dbWriteTable(con, "comorbidade_paciente", df_tabela_comorbidades_paciente, row.names=FALSE, append=TRUE)
}
#Doenca Neurologica
df_micro_dados_casos_filter <- filter(df_micro_dados_casos, df_micro_dados_casos["Doenca Neurologica"]=="SIM")
if (nrow(df_micro_dados_casos_filter) > 0) {
  df_tabela_comorbidades_paciente <- 
    data.frame(
      select(df_micro_dados_casos_filter, 
             codigo_paciente), 
      select(filter(df_tabela_comorbidades, descricao=="Doenca Neurologica"), 
             codigo)
    )
  df_tabela_comorbidades_paciente <- rename(df_tabela_comorbidades_paciente, comorbidade=codigo)
  df_tabela_comorbidades_paciente <- rename(df_tabela_comorbidades_paciente, paciente=codigo_paciente)
  dbWriteTable(con, "comorbidade_paciente", df_tabela_comorbidades_paciente, row.names=FALSE, append=TRUE)
}
#Doenca Renal
df_micro_dados_casos_filter <- filter(df_micro_dados_casos, df_micro_dados_casos["Doenca Renal"]=="SIM")
if (nrow(df_micro_dados_casos_filter) > 0) {
  df_tabela_comorbidades_paciente <- 
    data.frame(
      select(df_micro_dados_casos_filter, 
             codigo_paciente), 
      select(filter(df_tabela_comorbidades, descricao=="Doenca Renal"), 
             codigo)
    )
  df_tabela_comorbidades_paciente <- rename(df_tabela_comorbidades_paciente, comorbidade=codigo)
  df_tabela_comorbidades_paciente <- rename(df_tabela_comorbidades_paciente, paciente=codigo_paciente)
  dbWriteTable(con, "comorbidade_paciente", df_tabela_comorbidades_paciente, row.names=FALSE, append=TRUE)
}
#Imunodepressao
df_micro_dados_casos_filter <- filter(df_micro_dados_casos, Imunodepressao=="SIM")
if (nrow(df_micro_dados_casos_filter) > 0) {
  df_tabela_comorbidades_paciente <- 
    data.frame(
      select(df_micro_dados_casos_filter, 
             codigo_paciente), 
      select(filter(df_tabela_comorbidades, descricao=="Imunodepressao"), 
             codigo)
    )
  df_tabela_comorbidades_paciente <- rename(df_tabela_comorbidades_paciente, comorbidade=codigo)
  df_tabela_comorbidades_paciente <- rename(df_tabela_comorbidades_paciente, paciente=codigo_paciente)
  dbWriteTable(con, "comorbidade_paciente", df_tabela_comorbidades_paciente, row.names=FALSE, append=TRUE)
}
#Obesidade
df_micro_dados_casos_filter <- filter(df_micro_dados_casos, Obesidade=="SIM")
if (nrow(df_micro_dados_casos_filter) > 0) {
  df_tabela_comorbidades_paciente <- 
    data.frame(
      select(df_micro_dados_casos_filter, 
             codigo_paciente), 
      select(filter(df_tabela_comorbidades, descricao=="Obesidade"), 
             codigo)
    )
  df_tabela_comorbidades_paciente <- rename(df_tabela_comorbidades_paciente, comorbidade=codigo)
  df_tabela_comorbidades_paciente <- rename(df_tabela_comorbidades_paciente, paciente=codigo_paciente)
  dbWriteTable(con, "comorbidade_paciente", df_tabela_comorbidades_paciente, row.names=FALSE, append=TRUE)
}
#Pneumopatia
df_micro_dados_casos_filter <- filter(df_micro_dados_casos, Pneumopatia=="SIM")
if (nrow(df_micro_dados_casos_filter) > 0) {
  df_tabela_comorbidades_paciente <- 
    data.frame(
      select(df_micro_dados_casos_filter, 
             codigo_paciente), 
      select(filter(df_tabela_comorbidades, descricao=="Pneumopatia"), 
             codigo)
    )
  df_tabela_comorbidades_paciente <- rename(df_tabela_comorbidades_paciente, comorbidade=codigo)
  df_tabela_comorbidades_paciente <- rename(df_tabela_comorbidades_paciente, paciente=codigo_paciente)
  dbWriteTable(con, "comorbidade_paciente", df_tabela_comorbidades_paciente, row.names=FALSE, append=TRUE)
}
#Puérpera
df_micro_dados_casos_filter <- filter(df_micro_dados_casos, Puérpera=="SIM")
if (nrow(df_micro_dados_casos_filter) > 0) {
  df_tabela_comorbidades_paciente <- 
    data.frame(
      select(df_micro_dados_casos_filter, 
             codigo_paciente), 
      select(filter(df_tabela_comorbidades, descricao=="Puérpera"), 
             codigo)
      
    )
  df_tabela_comorbidades_paciente <- rename(df_tabela_comorbidades_paciente, comorbidade=codigo)
  df_tabela_comorbidades_paciente <- rename(df_tabela_comorbidades_paciente, paciente=codigo_paciente)
  dbWriteTable(con, "comorbidade_paciente", df_tabela_comorbidades_paciente, row.names=FALSE, append=TRUE)
}
#Síndrome De Down
df_micro_dados_casos_filter <- filter(df_micro_dados_casos, df_micro_dados_casos["Síndrome De Down"]=="SIM")
if (nrow(df_micro_dados_casos_filter) > 0) {
  df_tabela_comorbidades_paciente <- 
    data.frame(
      select(df_micro_dados_casos_filter, 
             codigo_paciente), 
      select(filter(df_tabela_comorbidades, descricao=="Síndrome De Down"), 
             codigo)
    )
  df_tabela_comorbidades_paciente <- rename(df_tabela_comorbidades_paciente, comorbidade=codigo)
  df_tabela_comorbidades_paciente <- rename(df_tabela_comorbidades_paciente, paciente=codigo_paciente)
  dbWriteTable(con, "comorbidade_paciente", df_tabela_comorbidades_paciente, row.names=FALSE, append=TRUE)
}
#Outros Fatores De Risco
df_micro_dados_casos_filter <- filter(df_micro_dados_casos, df_micro_dados_casos["Outros Fatores De Risco"]=="SIM")
if (nrow(df_micro_dados_casos_filter) > 0) {
  df_tabela_comorbidades_paciente <- 
    data.frame(
      select(df_micro_dados_casos_filter, 
             codigo_paciente), 
      select(filter(df_tabela_comorbidades, descricao=="Outros Fatores De Risco"), 
             codigo)
    )
  df_tabela_comorbidades_paciente <- rename(df_tabela_comorbidades_paciente, comorbidade=codigo)
  df_tabela_comorbidades_paciente <- rename(df_tabela_comorbidades_paciente, paciente=codigo_paciente)
  dbWriteTable(con, "comorbidade_paciente", df_tabela_comorbidades_paciente, row.names=FALSE, append=TRUE)
}
#Casos
df_tabela_caso <- select(df_micro_dados_casos, 
                         diagnostico="Diagnostico Covid19", 
                         data_inicio_sintomas="Data Inicio Sintomas",
                         obito=Obito, 
                         paciente=codigo_paciente)
df_tabela_caso$codigo = seq(1,nrow(df_tabela_caso))
dbWriteTable(con, "caso", df_tabela_caso, row.names=FALSE, append=TRUE)

#ISOLAMENTO
df_isolamento <- read_csv2(
  paste0(diretorio_arquivos_csv, "20210510_isolamento.csv"), 
  locale = locale(encoding = "UTF-8")
)

#Prepara e insere dados de isolamento
df_isolamento$'Média de Índice De Isolamento' <- as.double(df_isolamento$'Média de Índice De Isolamento') * 100

#Ajusta data isolamento
df_isolamento <- df_isolamento %>% 
  separate( 
    col = Data, 
    into = c("diaSemana", "diames"),
    sep = ","
  )
# separando o dia e mês
df_isolamento <- df_isolamento %>% 
  separate( 
    col = diames, 
    into = c("dia", "mes"),
    sep = "/"
  ) 
#criando colunas para comparação
df_isolamento$data1 <- paste0("2020-", df_isolamento$mes,"-", str_trim(df_isolamento$dia))
df_isolamento$data2 <- paste0("2021-", df_isolamento$mes,"-", str_trim(df_isolamento$dia))
# ifelse para comparar o dia da semana das datas com o dia da semana da tabela 
df_isolamento$dataFinal <- 
  ifelse(format.Date(as.Date(df_isolamento$data1), "%A") == str_trim(df_isolamento$diaSemana), 
         df_isolamento$data1, 
         df_isolamento$data2)

df_tabela_isolamento <- select(df_isolamento, 
                               data_isolamento=dataFinal,
                               media_isolamento="Média de Índice De Isolamento",
                               municipio="Código Município IBGE")
df_tabela_isolamento$codigo <- seq(1,nrow(df_tabela_isolamento))
df_tabela_isolamento <- filter(df_tabela_isolamento, municipio != 35)
dbWriteTable(con, "isolamento", df_tabela_isolamento, row.names=FALSE, append=TRUE)

print("Registros inseridos no banco de dados")


############################################################################################################


#função para arrumar o encoding 
set_utf8 <- function(x) {
  # Declare UTF-8 encoding on all character columns:
  chr <- sapply(x, is.character)
  x[, chr] <- lapply(x[, chr, drop = FALSE], `Encoding<-`, "UTF-8")
  # Same on column names:
  Encoding(names(x)) <- "UTF-8"
  x
}

#Gera tabelas
### TABELAS

### tabela consulta 1 - Casos, óbitos, letalidade, mortalidade por Região Administativa
df1 <- as_tibble(
  set_utf8(
    dbGetQuery(con,
               "select
                regiao_administrativa.codigo, 
                regiao_administrativa.nome,
                min(pop.populacao) as populacao,
                count(distinct municipio.ibge) municipios,
                sum(case when obito then 1 else 0 end) obitos,
                count(1) casos
                from caso
                inner join paciente on paciente.codigo = caso.paciente
                inner join municipio on municipio.ibge = paciente.municipio
                inner join regiao_administrativa on regiao_administrativa.codigo = municipio.regiao_administrativa 
                inner join (select regiao_administrativa, sum(populacao) as populacao 
                            from municipio 
                            group by regiao_administrativa
                            ) as pop on regiao_administrativa.codigo = pop.regiao_administrativa 
                group by 
                regiao_administrativa.codigo, 
                regiao_administrativa.nome;")))

#preparando a tabela 
tab_df1 <- df1 %>%
  select(nome, obitos, casos, populacao) %>%
  mutate(letalidade = obitos/casos) %>%
  mutate(mortalidade = obitos*100000/populacao)

colnames(tab_df1) <- c("Região Administrativa", "Número de óbitos", 
                       "Número de casos","População", "Letalidade", 
                       "Mortalidade/100 mil habit.")

# gerando a tabela para impressão
gt(tab_df1) %>%
  tab_header("Casos, óbitos,letalidade e mortalidade de Covid-19 por Região Administrativa de São Paulo")%>%
  tab_source_note("Fonte: Dados abertos/Governo de São Paulo")%>%
  fmt_percent(columns = c("Letalidade"), decimals = 2, dec_mark = ",") %>%
  fmt_number(columns = c("Número de óbitos", "Número de casos", "População"), decimals = 0,sep_mark = ".")%>%
  fmt_number(columns = "Mortalidade/100 mil habit.", decimals = 1, dec_mark = ",")


### tabela consulta 2 - comorbidade, casos e óbitos por gênero

df2 <- as_tibble(set_utf8(dbGetQuery(con,
                                     "select 
                                      comorbidade_codigo, 
                                      comorbidade_descricao, 
                                      genero,
                                      sum(case when obito then 1 else 0 end) obitos,
                                      count(1) casos,
                                      cast(
                                        (
                                          cast(sum(case when obito then 1 else 0 end) * 100 as numeric(38,2)) / 
                                          cast(count(1) as numeric(38,2))
                                        ) as numeric(5,2)
                                      ) porcentagem
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
                                      
                                      group by 
                                      comorbidade_codigo, 
                                      comorbidade_descricao, 
                                      genero
                                      order by 
                                      comorbidade_descricao")))

#preparando a tabela 
tab_df2 <- mutate(df2, letalidade = obitos/casos) %>%
  select(comorbidade_descricao, genero, letalidade)

tab_df2$comorbidade_descricao <- ifelse(is.na(tab_comorbGenero$comorbidade_descricao), "Geral sem comorb.", tab_comorbGenero$comorbidade_descricao)
tab_df2$comorbidade_descricao <- ifelse(tab_comorbGenero$comorbidade_descricao == "Doenca Hematologica", "Doença Hematológica", tab_comorbGenero$comorbidade_descricao)
tab_df2$comorbidade_descricao <- ifelse(tab_comorbGenero$comorbidade_descricao == "Doenca Hepatica", "Doença Hepática", tab_comorbGenero$comorbidade_descricao)
tab_df2$comorbidade_descricao <- ifelse(tab_comorbGenero$comorbidade_descricao == "Doenca Neurologica", "Doença Neurológica", tab_comorbGenero$comorbidade_descricao)
tab_df2$comorbidade_descricao <- ifelse(tab_comorbGenero$comorbidade_descricao == "Imunodepressao", "Imunodepressão", tab_comorbGenero$comorbidade_descricao)

tab_df2M <- tab_df2 %>%
  filter(genero == "MASCULINO") %>%
  select(comorbidade_descricao, letalidade) %>%
  rename(letalidade_masculino = letalidade)

tab_df2F <- tab_df2 %>%
  filter(genero == "FEMININO") %>%
  select(comorbidade_descricao, letalidade) %>%
  rename(letalidade_feminino = letalidade) 


tab_df2G <- inner_join(tab_df2M,tab_df2F, by="comorbidade_descricao")
colnames(tab_df2G) <- c("Comorbidade","Letal. Masculino", "Letal. Feminino")

# gerando a tabela para impressão
gt(tab_df2G) %>%
  tab_header("Letalidade de Covid-19 por comorbidade e gênero em São Paulo")%>%
  tab_source_note("Fonte: Dados abertos/Governo de São Paulo")%>%
  tab_spanner(label="Letalidade", columns = c("Letal. Masculino", "Letal. Feminino"))%>%
  fmt_percent(columns = c("Letal. Masculino", "Letal. Feminino"), decimals = 2, dec_mark = ",") %>%
  cols_label("Letal. Masculino" = html("Gênero Masculino"), "Letal. Feminino" = html("Gênero Feminino"))


### tabela consulta 3 - Faixa etária, casos e óbitos por  gênero

df3 <- as_tibble(set_utf8(dbGetQuery(con,
                                     "select 
                                      (case when paciente.idade <= 18 then '01. 0 a 18'
                                        	  when paciente.idade <= 23 then '02. 19 a 23'
                                        	  when paciente.idade <= 28 then '03. 24 a 28'
                                        	  when paciente.idade <= 33 then '04. 29 a 33'
                                        	  when paciente.idade <= 38 then '05. 34 a 38'
                                        	  when paciente.idade <= 43 then '06. 39 a 43'
                                        	  when paciente.idade <= 48 then '07. 44 a 48'
                                        	  when paciente.idade <= 53 then '08. 49 a 53'
                                        	  when paciente.idade <= 58 then '09. 54 a 58'
                                        	  else '10. + de 59 anos'
                                      end) idade,
                                      paciente.genero,
                                      sum(case when obito then 1 
                                               else 0 
                                          end) obitos,
                                      count(1) casos
                                      from caso
                                      inner join paciente on caso.paciente = paciente.codigo 
                                      where paciente.genero = 'MASCULINO' or paciente.genero = 'FEMININO'
                                      group by 
                                      1, 
                                      paciente.genero")))

#preparando a tabela 
tab_df3 <- df3 %>%
  mutate(df3, letalidade = obitos/casos) %>%
  select(idade, genero, letalidade)

tab_df3M <- tab_df3 %>%
  filter(genero == "MASCULINO") %>%
  select(idade, letalidade) %>%
  rename(letalidade_masculino = letalidade)

tab_df3F <-tab_df3 %>%
  filter(genero == "FEMININO") %>%
  select(idade, letalidade) %>%
  rename(letalidade_feminino = letalidade)

tab_df3G <- inner_join(tab_df3M,tab_df3F, by="idade")

colnames(tab_df3G ) <- c("Faixa etária (anos)","Letal. Masculino", "Letal. Feminino")

# gerando a tabela para impressão
gt(tab_df3G) %>%
  tab_header("Letalidade de Covid-19 por faixa etária e gênero em São Paulo")%>%
  tab_source_note("Fonte: Dados abertos/Governo de São Paulo")%>%
  tab_spanner(label="Letalidade", columns = c("Letal. Masculino", "Letal. Feminino"))%>%
  fmt_percent(columns = c("Letal. Masculino", "Letal. Feminino"), decimals = 2, dec_mark = ",") %>%
  cols_label("Letal. Masculino" = html("Gênero Masculino"), "Letal. Feminino" = html("Gênero Feminino"))


############# consulta 4 - Isolamento

df4 <- as_tibble(set_utf8(dbGetQuery(con,
                                     "select
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
                                      right join isolamento on isolamento.municipio = municipio.ibge 
                                             and caso.data_inicio_sintomas = isolamento.data_isolamento
                                      where data_inicio_sintomas is not null
                                      group by 
                                      regiao_administrativa.codigo, 
                                      regiao_administrativa.nome, 
                                      caso.data_inicio_sintomas,
                                      isolamento.data_isolamento
                                      order by 
                                      caso.data_inicio_sintomas")))



df4 <- inner_join(df4,select(df1, nome, populacao), by='nome')

df4 <- mutate(df4,incidencia = casos*1000000 / populacao)

df4gp <- df4%>%
  mutate(ano = format(data_inicio_sintomas,"%Y"), mes=format(data_inicio_sintomas,"%m"),
         taxa_isolamento = taxa_isolamento/100) %>%
  group_by(nome, ano, mes) %>%
  summarise( mediaIsol = mean(taxa_isolamento),mediaCasos = mean(casos), mediaInc = mean(incidencia))

#view(df4gp)

colnames(df4gp) <- c("Região Administrativa", "Ano", "Mês","Média de Isolamento","Média de Casos", "Média de incidência/milhão hab.")


#summary(df4gp)

# gerando a tabela para impressão
gt(df4gp) %>%
  tab_header("Média mensal de isolamento, casos e incidência de Covid-19 por Região Administrativa no Estado de São Paulo")%>%
  tab_source_note("Fonte: Dados abertos/Governo de São Paulo")%>%
  fmt_percent(columns = c("Média de Isolamento"), decimals = 1, dec_mark = ",", sep_mark = ".") %>%
  fmt_number(columns = c("Média de Casos","Média de incidência/milhão hab."), decimals = 1, dec_mark = ",", sep_mark = ".")



########  Gráficos do isolamento x Incidência


coeff <- max(df4$incidencia)/100

ggplot(df4) +
  
  geom_bar(mapping = aes(x=data_inicio_sintomas, y=incidencia), stat="identity", size=.1, fill="red", alpha=.6) + 
  
  geom_line(mapping = aes(x=data_inicio_sintomas,y=coeff*taxa_isolamento), size=0.5, color="blue") +
  
  scale_x_date(name = "Data")+
  
  scale_y_continuous(
    # Features of the first axis
    name = "Incidência de COvid-19 (por milhão de hab.)",
    
    # Add a second axis and specify its features
    sec.axis = sec_axis(~ . * 1/coeff , name="Taxa de isolamento (%)")
  ) +
  theme_bw() +
  theme(
    axis.title.y = element_text(color = "red", size=13),
    axis.title.y.right = element_text(color = "blue", size=13),
    axis.text.x = element_text(angle = 45, hjust = 1))  +
  ggtitle("Incidência de Covid-19 e taxa de isolamento ao longo do tempo por RA") +
  facet_wrap(~ nome, scales = "free_y")

## Objetivos 1 e 2
df1<-dbGetQuery(con, "SELECT 
                      regiao_administrativa.codigo, 
                      regiao_administrativa.nome,
                      count(distinct municipio.ibge) municipios,
                      sum(case when obito then 1 else 0 end) obitos,
                      count(1) casos
                      from caso
                      inner join paciente on paciente.codigo = caso.paciente
                      inner join municipio on municipio.ibge = paciente.municipio
                      inner join regiao_administrativa on regiao_administrativa.codigo = municipio.regiao_administrativa 
                      group by 
                      regiao_administrativa.codigo, 
                      regiao_administrativa.nome")
df1<-set_utf8(df1)


p1<-arrange(df1, casos) %>% #ordenar do maior para menor obitos e casos; mas não ordena os nomes
  mutate(nome=factor(nome, levels=nome)) %>% #atualizar os nomes
  ggplot() + geom_col(aes(x=nome, y=casos, group=nome)) +
  coord_flip() + # mudar a posição dos eixos
  labs(y="Número de Casos", title = "Número de casos no estado de São Paulo") +
  theme_minimal() + theme(axis.title.y = element_blank()) +
  scale_y_continuous(limits = c(0,1300000), expand = c(0, 0))

p1

p2<-arrange(df1, obitos) %>% #ordenar do maior para menor obitos e casos; mas não ordena os nomes
  mutate(nome=factor(nome, levels=nome)) %>% #atualizar os nomes
  ggplot() + theme_minimal() + geom_col(aes(x=nome, y=obitos, group=nome)) +
  coord_flip() + # mudar a posição dos eixos
  labs(y="Número de Óbitos", title = "Número de óbitos no estado de São Paulo", 
       caption = expression(paste("*RA: Região Administrativa e RM: Região Metropolitana \n Fonte: Dados Abertos/Governo de São Paulo"))) +
  theme(plot.caption = element_text(hjust = -2.5)) +
  theme(axis.title.y = element_blank()) +
  scale_y_continuous(limits = c(0,55000), expand = c(0, 0))

p2

plots_casos_obitos_regiao<-wrap_plots(p1,p2)
ggsave("objetivo1e2.png", width=30, height=15, units="cm", dpi=300)


# Mapas
df6<-dbGetQuery(con, "select
                      municipio.ibge, 
                      municipio.nome,
                      municipio.latitude, 
                      municipio.longitude,
                      sum(case when obito then 1 else 0 end) obitos,
                      count(1) casos
                      from caso
                      inner join paciente on paciente.codigo = caso.paciente
                      inner join municipio on municipio.ibge = paciente.municipio
                      group by 
                      municipio.ibge, 
                      municipio.nome, 
                      municipio.latitude, 
                      municipio.longitude;")

sp_muni<-read_municipality(code_muni = "SP", year = 2020)

data_mapa<-left_join(sp_muni, df6, by=c("code_muni"="ibge"))

p<-ggplot() + geom_sf(data=data_mapa, fill="#2D3E50", size=.15, show.legend = FALSE) +
  theme_minimal()

p1 <- p + geom_point(data=data_mapa, aes(x = longitude, y = latitude, size= casos, color=casos), alpha = 0.5) +
  scale_size(range = c(1, 10)) +
  guides(size=FALSE) +
  scale_color_gradient2(low = "white", mid = "yellow", high = "red", midpoint = 200000 ,name="Nº de casos")+
  theme_minimal() +
  theme(legend.position = c(0.9, 0.80))+
  annotation_scale() +
  labs(title = "Distribuição de casos de COVID-19 no estado de São Paulo ")


p2 <- p + geom_point(data=data_mapa, aes(x = longitude, y = latitude, size=obitos, color=obitos), alpha = 0.5) +
  scale_size(range = c(1, 10)) +
  guides(size=FALSE) +
  scale_color_gradient2(low = "white", mid = "yellow", high = "red", midpoint = 10000, name="Nº de óbitos")+
  theme_minimal() +
  theme(legend.position = c(0.9, 0.80))+
  annotation_scale() +
  labs(title = "Distribuição de óbitos por COVID-19 no estado de São Paulo ", caption = "Fonte: Dados Abertos/Governo de São Paulo")

plots_casos_obitos_regiao<-wrap_plots(p1,p2)
ggsave("objetivo1e2mapas.png", width=35, height=15, units="cm", dpi=300)

# versão interativa
library(plotly)

p1 <- p + geom_point(data=data_mapa, aes(x = longitude, y = latitude, size= casos, color=nome), alpha = 0.5) +
  theme_minimal() +
  scale_size(range = c(1, 10)) +
  theme(legend.position = "none")+
  labs(title = "Distribuição de casos de COVID-19 no estado de São Paulo ")

ggplotly(p1)

p2 <- p + geom_point(data=data_mapa, aes(x = longitude, y = latitude, size= obitos, color=nome), alpha = 0.5) +
  theme_minimal() +
  scale_size(range = c(1, 10)) +
  theme(legend.position = "none")+
  labs(title = "Distribuição de óbitos por COVID-19 no estado de São Paulo ")

ggplotly(p2)


## Objetivo 3
df5<-dbGetQuery(con, "SELECT comorbidade_codigo, 
                      comorbidade_descricao, 
                      genero,
                      sum(case when obito then 1 else 0 end) obitos,
                      count(1) casos,
                      cast(
                        (
                          cast(sum(case when obito then 1 else 0 end) * 100 as numeric(38,2)) / 
                          cast(count(1) as numeric(38,2)
                        )
                      ) as numeric(5,2)) porcentagem
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
                      group by 
                      comorbidade_codigo, 
                      comorbidade_descricao, 
                      genero 
                      order by 
                      comorbidade_descricao")
df5<-set_utf8(df5)

p1<-mutate_at(df5, vars(comorbidade_descricao, genero), list(factor)) %>%
  ggplot() + theme_minimal() + 
  geom_col(aes(x=genero, y=porcentagem, fill=genero)) +
  labs(y="Taxa de letalidade (%)", title = "Taxa de letalidade por COVID-19 em relação ao gênero e à comorbidade informada", 
       caption = "Fonte: Dados Abertos/Governo de São Paulo") +
  theme(axis.title.x = element_blank()) +
  theme(legend.position = "none") +
  scale_fill_brewer(palette = "Pastel2") +
  facet_wrap(~comorbidade_descricao, scales = "free_y")
p1
ggsave("objetivo3.png", width=20, height=17, units="cm", dpi=300)


## Objetivo 4
df3<-dbGetQuery(con, "SELECT (case when paciente.idade <= 18 then '0 a 18'
                                   when paciente.idade <= 23 then '18 a 23'
                                   when paciente.idade <= 28 then '24 a 28'
                                   when paciente.idade <= 33 then '29 a 33'
                                   when paciente.idade <= 38 then '33 a 38'
                                   when paciente.idade <= 43 then '39 a 43'
                                   when paciente.idade <= 48 then '44 a 48'
                                   when paciente.idade <= 53 then '49 a 53'
                                   when paciente.idade <= 58 then '54 a 58'
                                   else '+ de 59 anos' 
                              end) idade,
                              paciente.genero,
                              sum(case when obito then 1 
                                       else 0 
                                  end) obitos,
                              count(1)
                              from caso
                              inner join paciente on caso.paciente = paciente.codigo 
                              where paciente.genero = 'MASCULINO' or paciente.genero = 'FEMININO'
                              group by 
                              1, 
                              paciente.genero")

class(df3$idade)
class(df3$genero)

positions <- c("0 a 18", "18 a 23", "24 a 28", 
               "29 a 33", "33 a 38", "39 a 43", 
               "44 a 48", "49 a 53", "54 a 58", 
               "+ de 59 anos")

p1<-mutate_at(df3, vars(idade, genero), list(factor)) %>%
  ggplot(aes(x=idade, y=obitos, fill=genero)) + theme_minimal() +
  geom_bar(stat="identity", position=position_dodge()) +
  scale_x_discrete(limits = positions) +
  scale_fill_brewer(palette = "Pastel2") +
  theme(legend.title = element_blank(), legend.justification=c(1,0), legend.position=c(0.95,0.06)) +
  theme(legend.background = element_blank())+
  scale_y_continuous(limits = c(0,42000), expand = c(0, 0)) +
  labs(x="Idade", y="Número de óbitos", title = "Número de óbitos relacionados à COVID-19 por faixa etária no estado de São Paulo" ,caption = "Fonte: Dados Abertos/Governo de São Paulo") +
  coord_flip()
p1

ggsave("objetivo4.png", width=21, height=16, units="cm", dpi=300)


## Objetivo 5
df4<-dbGetQuery(con, "SELECT 
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
                      right join isolamento on isolamento.municipio = municipio.ibge 
                                           and caso.data_inicio_sintomas = isolamento.data_isolamento
                      where data_inicio_sintomas is not null
                      group by 
                      regiao_administrativa.codigo, 
                      regiao_administrativa.nome, 
                      data_inicio_sintomas, 
                      isolamento.data_isolamento")
df4<-set_utf8(df4)

p1<-mutate_at(df4, vars(nome), list(factor)) %>%
  mutate(taxa_isolamento, porcentagem=taxa_isolamento*100) %>%
  ggplot(aes(x=porcentagem, y=obitos)) + theme_minimal() +
  geom_point(colour="black", shape=21, size=2) +
  geom_point(fill="black", shape=21, size=2, alpha=0.3) +
  geom_smooth(aes(x=porcentagem, y=obitos), se=FALSE) +
  facet_wrap(~nome, scales = "free_y") + 
  labs(x="Taxa de isolamento (%)", 
       y="Número de óbitos", 
       title = "Relação entre número de óbitos por COVID-19 e a taxa de isolamento no estado de São Paulo ", 
       caption = "Fonte: Dados Abertos/Governo de São Paulo")
p1

ggsave("objetivo5a.png", width=21, height=17, units="cm", dpi=300)