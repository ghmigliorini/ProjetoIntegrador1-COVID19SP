-- Cria base de dados
create database projetointegradorcovid;

-- Cria tabela de comorbidades
create table comorbidade (
	codigo integer not null,
	descricao varchar(100) not null,
	constraint pk_comorbidade primary key (codigo)
);

-- Cria tabela de regiões administrativas
create table regiao_administrativa (
	codigo integer not null,
	nome varchar(100) not null,
	constraint pk_regiao_administrativa primary key(codigo)
);

-- Cria tabela de municípios
create table municipio (
	ibge integer not null,
	nome varchar(100) not null,
	regiao_administrativa integer,
	populacao integer,
	latitude decimal(9,6),
	longitude decimal(9,6),
	constraint pk_municipio primary key (ibge),
	constraint fk_regiao_administrativa_municipio foreign key (regiao_administrativa) references regiao_administrativa(codigo)
);

-- Cria tabela de pacientes
create table paciente (
	codigo integer not null,
	genero varchar(15),
	idade smallint,
	municipio integer,
	constraint pk_paciente primary key (codigo),
	constraint fk_municipio_paciente foreign key (municipio) references municipio(ibge)
);

-- Cria tabela de comorbidade do paciente
create table comorbidade_paciente (
	paciente integer not null,
	comorbidade integer not null,
	constraint pk_comorbidade_paciente primary key (paciente, comorbidade),
	constraint fk_paciente_comorbidade_paciente foreign key (paciente) references paciente(codigo),
	constraint fk_comorbidade_comorbidade_paciente foreign key (comorbidade) references comorbidade(codigo)
);

-- Cria tabela caso
create table caso (
	codigo integer not null,
	diagnostico varchar(15) not null,
	data_inicio_sintomas date,
	obito boolean not null,
	paciente integer not null,
	constraint pk_caso primary key (codigo),
	constraint fk_paciente_caso foreign key (paciente) references paciente(codigo)
);

-- Cria tabela isolamento
create table isolamento (
	codigo integer not null,
	data_isolamento date not null,
	media_isolamento smallint not null,
	municipio integer not null,
	constraint pk_isolamento primary key (codigo),
	constraint fk_municipio_isolamento foreign key (municipio) references municipio(ibge)
);