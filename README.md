# Data_eng

Objetivo


O principal objetivo desse trabalho é preparar dados que foram extraidos de uma fonte de dados internet utilizando plataformas em nuvem,  a partir desses dados estruturados em nuvem  realizar consultas para responder algumas perguntas chave até 12 de julho de 2024.


Plataforma
 A plataforma escolhida para atingir os objetivos traçados no trabalho foi o Data bricks comunity edition em conjunto com o google cloud plataform para garantir que os clusters tenham uma maquina em nuvem  para rodar. é importante destacar que a ferramenta databricks comunity edition é possivel ser utilizada por apenas 14 dias e o google cloud platform possui uma restrição de até 300 dolares na sua versão gratuita mas isso não será uma restrição para o projeto devido ao nivel de complexidade.



Detalhamento
1. Busca pelos dados

 Para a escolha da fonte de dados, foi fornecido pela instituição de ensino algumas alternativas gratuitas disponiveis na intert com base na lista a seguir:
 
Google Cloud Public Datasets (https://cloud.google.com/datasets)
Kaggle (https://www.kaggle.com/datasets)
Portal da Transparência (https://portaldatransparencia.gov.br/)
IMDB (https://datasets.imdbws.com/)
Tableau (https://www.tableau.com/learn/articles/free-public-data-sets)
Stanford Large Network Dataset Collection (https://snap.stanford.edu/data/index.html)
Yelp Open Dataset (https://www.yelp.ca/dataset).

 Dentro das opções, a base de dados que se mostrou mais interessante para a realização do trabalho foi a "Visualizing Workforce Trends in Europe's Film" disponibilizada na kaggle no seguinte link https://www.kaggle.com/code/nileshely/visualizing-workforce-trends-in-europe-s-film/input . O conjunto de dados disponivel possui 20 tabelas relacionais com informações sobre filmes produzidos, empresa , região, tipo de organização, funcionários e gestão . Todas essas informações são disponibilizadas em arquivos CSV , o que facilitou todo processo de analise das informações.


2. Coleta

 Pelo fato dos dados já estarem disponiveis em CSV o processo de carga dessas informações para uma plataforma em nuvem foi relativamente simples, apenas foi feito o export das tabelas para minha maquina para que após a modelagem dos dados fosse realizada. 

  Os dados também estão disponiveis na pasta "dados" desse repositório.
 

3. Modelagem


 Para ter as informações de forma mais visual o possivel , optei por utilizar o sistema WEB sugerido nas aulas do curso "https://app.genmymodel.com/api/" , Atraves dele , foi possivel construir toda a estrutura de tabelas e campos , assim como identificar de forma mais simples as relações existentes entre as tabelas . É possivel consultar todos os arquivos gerados dentro da pasta "modelagem" dentro desse repositório.

  Para a construção do catalogo de dados foi feito uma bela em um arquivo em docs que também está disponivel nas pasta de modelagem desse repositório com maiores informações sobre os dados que foram utilizados para realizar a consulta. Importante destacar que devido elevado volume de dados fornecidos nas tabelas apenas foi realizado a descrição dos campos que teriam relevancia para o trabalho.


 

4. Carga

Após a carga dos dados na plataforma e com a modelagem dos dados estruturada ,  foi realizado a estruturação do data lake house dentro do databricks utilizando o Spark, nessa etapa foi utilizado o formato de dados "delta" para acompanhar os históricos de versões caso existesse alguma alteração nos dados. Os arquivos gerados ficaram todos salvos em um pasta "delta" dentro do repositório do databricks.

Codigo da etapa:

City = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("/FileStore/tables/Datalake/city.csv")
City.write.format("delta").mode("overwrite").save("/FileStore/tables/Datalake/delta/city_delta")

Company = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("/FileStore/tables/Datalake/company.csv")
Company.write.format("delta").mode("overwrite").save("/FileStore/tables/Datalake/delta/company_delta")

Company_film = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("/FileStore/tables/Datalake/company_film.csv")
Company_film.write.format("delta").mode("overwrite").save("/FileStore/tables/Datalake/delta/company_film_delta")

Company_grant = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("/FileStore/tables/Datalake/company_grant.csv")
Company_grant.write.format("delta").mode("overwrite").save("/FileStore/tables/Datalake/delta/company_grant_delta")

Company_shareholder = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("/FileStore/tables/Datalake/company_shareholder.csv")
Company_shareholder.write.format("delta").mode("overwrite").save("/FileStore/tables/Datalake/delta/company_shareholder_delta")

Country = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("/FileStore/tables/Datalake/country.csv")
Country.write.format("delta").mode("overwrite").save("/FileStore/tables/Datalake/delta/country_delta")

Crew = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("/FileStore/tables/Datalake/crew.csv")
Crew.write.format("delta").mode("overwrite").save("/FileStore/tables/Datalake/delta/crew_delta")

Crew_info = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("/FileStore/tables/Datalake/crew_info.csv")
Crew_info.write.format("delta").mode("overwrite").save("/FileStore/tables/Datalake/delta/crew_info_delta")

Department = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("/FileStore/tables/Datalake/department.csv")
Department.write.format("delta").mode("overwrite").save("/FileStore/tables/Datalake/delta/department_delta")

Department_address = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("/FileStore/tables/Datalake/department_address.csv")
Department_address.write.format("delta").mode("overwrite").save("/FileStore/tables/Datalake/delta/department_address_delta")

Employee = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("/FileStore/tables/Datalake/employee.csv")
Employee.write.format("delta").mode("overwrite").save("/FileStore/tables/Datalake/delta/employee_delta")

Film = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("/FileStore/tables/Datalake/film.csv")
Film.write.format("delta").mode("overwrite").save("/FileStore/tables/Datalake/delta/film_delta")

Grant_request = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("/FileStore/tables/Datalake/grant_request.csv")
Grant_request.write.format("delta").mode("overwrite").save("/FileStore/tables/Datalake/delta/grant_request_delta")

Kind_of_organization = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("/FileStore/tables/Datalake/kind_of_organization.csv")
Kind_of_organization.write.format("delta").mode("overwrite").save("/FileStore/tables/Datalake/delta/kind_of_organization_delta")

Phone_number = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("/FileStore/tables/Datalake/phone_number.csv")
Phone_number.write.format("delta").mode("overwrite").save("/FileStore/tables/Datalake/delta/phone_number_delta")

Registration_body = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("/FileStore/tables/Datalake/registration_body.csv")
Registration_body.write.format("delta").mode("overwrite").save("/FileStore/tables/Datalake/delta/registration_body_delta")

Role = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("/FileStore/tables/Datalake/role.csv")
Role.write.format("delta").mode("overwrite").save("/FileStore/tables/Datalake/delta/role_delta")

Shareholder = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("/FileStore/tables/Datalake/shareholder.csv")
Shareholder.write.format("delta").mode("overwrite").save("/FileStore/tables/Datalake/delta/shareholder_delta")

Staff = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("/FileStore/tables/Datalake/staff.csv")
Staff.write.format("delta").mode("overwrite").save("/FileStore/tables/Datalake/delta/staff_delta")

Staff_salary = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("delimiter", ",").load("/FileStore/tables/Datalake/staff_salary.csv")
Staff_salary.write.format("delta").mode("overwrite").save("/FileStore/tables/Datalake/delta/staff_salary_delta")

 Como o conjunto de dados possuia 20 tabelas e nem todas seriam necessárias para responder as perguntas delimitadas dentro do objetivo foram geradas 3 consultas cada uma com seus respectivos joins :

 1- Consulta de filmes por tipo de organização :

 Estruturação dos dados:
 
df_kind_of_organization_delta = spark.read.format("delta").load("/FileStore/tables/Datalake/delta/kind_of_organization_delta")
df_kind_of_organization_delta.createOrReplaceTempView("kind_of_organization")

df_film_delta = spark.read.format("delta").load("/FileStore/tables/Datalake/delta/film_delta")
df_film_delta.createOrReplaceTempView("film")

df_company_film_delta = spark.read.format("delta").load("/FileStore/tables/Datalake/delta/company_film_delta")
df_company_film_delta.createOrReplaceTempView("company_film")

df_company_delta = spark.read.format("delta").load("/FileStore/tables/Datalake/delta/company_delta")
df_company_delta.createOrReplaceTempView("company")

Join:

join_query = """
SELECT kind_of_organization.id AS kind_of_organization_ID, kind_of_organization.name AS Type_of_organization , 
company_film.company_id as company_film_company_id, company_film.film_movie_code as company_film_movie_code,  
film.movie_code as film_movie_code, film.title AS title, 
film.release_year as release_year, 
company.id as company_id, company.name as company_name, company.kind_of_organization_id as company_kind_of_organization_ID
FROM film
JOIN company_film ON film.movie_code = company_film.film_movie_code
JOIN company ON company_film.company_id = company.id
JOIN kind_of_organization ON company.kind_of_organization_id = kind_of_organization.id
"""

df_result = spark.sql(join_query)

# Write the result to a new Delta table
df_result.write.format("delta").mode("overwrite").save("dbfs:/FileStore/tables/Datalake/delta/join.delta")


2- Colaboradores por filme produzido:
Estruturação dos dados
df_employee_delta = spark.read.format("delta").load("/FileStore/tables/Datalake/delta/employee_delta")
df_employee_delta.createOrReplaceTempView("employee")

df_film_delta = spark.read.format("delta").load("/FileStore/tables/Datalake/delta/film_delta")
df_film_delta.createOrReplaceTempView("film")

df_company_film_delta = spark.read.format("delta").load("/FileStore/tables/Datalake/delta/company_film_delta")
df_company_film_delta.createOrReplaceTempView("company_film")

df_company_delta = spark.read.format("delta").load("/FileStore/tables/Datalake/delta/company_delta")
df_company_delta.createOrReplaceTempView("company")

df_role_delta = spark.read.format("delta").load("/FileStore/tables/Datalake/delta/role_delta")
df_role_delta.createOrReplaceTempView("role")

df_crew_info_delta = spark.read.format("delta").load("/FileStore/tables/Datalake/delta/crew_info_delta")
df_crew_info_delta.createOrReplaceTempView("crew_info")

Join

join_query2 = """
SELECT crew_info.crew_id AS crew_id, crew_info.movie_code AS movie_code 
, crew_info.hourly_rate, crew_info.completion_bonus,
company_film.company_id AS company_film_company_id, company_film.film_movie_code as company_film_movie_code,  
film.title AS film_title, film.release_year as film_release_year, 
company.id as company_id, company.name as company_name,
role.name as role_name,role.id as role_id,
employee.id as employee_id, employee.gender

FROM crew_info
JOIN employee ON crew_info.crew_id = employee.id 
JOIN role ON crew_info.role_id = role.id 
JOIN company_film ON crew_info.movie_code = company_film.film_movie_code
JOIN film ON company_film.film_movie_code = film.movie_code
JOIN company ON company_film.company_id = company.id
"""

df_result2 = spark.sql(join_query2)

# Write the result to a new Delta table
df_result2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("dbfs:/FileStore/tables/Datalake/delta/join2.delta")


 3- Time de gestão por tipo de organização:

 Estruturação dos dados:
 #Criar tabela para responder a pergunta : Analises de  gestão

df_staff_salary_delta = spark.read.format("delta").load("/FileStore/tables/Datalake/delta/staff_salary_delta")
df_staff_salary_delta.createOrReplaceTempView("staff_salary")

df_staff_delta = spark.read.format("delta").load("/FileStore/tables/Datalake/delta/staff_delta")
df_staff_delta.createOrReplaceTempView("staff")

df_company_film_delta = spark.read.format("delta").load("/FileStore/tables/Datalake/delta/company_film_delta")
df_company_film_delta.createOrReplaceTempView("company_film")

df_department_delta = spark.read.format("delta").load("/FileStore/tables/Datalake/delta/department_delta")
df_department_delta.createOrReplaceTempView("department")

df_department_address_delta = spark.read.format("delta").load("/FileStore/tables/Datalake/delta/department_address_delta")
df_department_address_delta.createOrReplaceTempView("department_address")

df_company_delta = spark.read.format("delta").load("/FileStore/tables/Datalake/delta/company_delta")
df_company_delta.createOrReplaceTempView("company")

df_kind_of_organization_delta = spark.read.format("delta").load("/FileStore/tables/Datalake/delta/kind_of_organization_delta")
df_kind_of_organization_delta.createOrReplaceTempView("kind_of_organization")

Join:

join_query3 = """
SELECT staff_salary.salary, staff_salary.working_hours,staff_salary.job_level,staff_salary.staff_id,
department.name AS department,
company.name AS company_name,
kind_of_organization.name AS kind_of_organization
FROM staff_salary
LEFT OUTER JOIN staff ON staff_salary.staff_id = staff.staff_id 
LEFT OUTER JOIN department ON staff.department_id = department.id 
LEFT OUTER JOIN department_address ON department.id = department_address.department_id
LEFT OUTER JOIN company ON company.id = department_address.company_id
LEFT OUTER JOIN kind_of_organization ON company.kind_of_organization_id = kind_of_organization.id
"""

df_result3 = spark.sql(join_query3)

df_result3.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("dbfs:/FileStore/tables/Datalake/delta/join3.delta")

5. Análise

          a. Qualidade de dados

 Os dados que foram disponibilizados pela plataforma estavam muito bem estruturados, posuindo relações em as tabalas de forma facil de identificar e não foi possivel identificar grandes erros de preenchimento das informações que geralmente são comuns em grandes quantidades de dados como Campos de texto onde deveriam ter apenas numeros ou datas em formatos diferentes. Isso acabou facilitando o trabalho o trabalho de analise dos dados para chegar aos objetivos que foram delimitados para o projeto. Contudo, apos analisar os resultados observamos que os resultados não condizem com a realidade , fazendo uma breve busca no google não e possivel identificar as informações que eram apresentadas em toda a base de dados . Isso significa que a base de dados está muito bem estruturada para fins de estudo, contudo para tirar soluções concretas para a tomada de decisão ela não pode ser levada em consideração.

          b. Solução do problema
          
Para melhor visualização dos resultado obtidos, anexei ao repositório um arquivo com as evidencias das consultas e graficos obtidos ao repositório


Autoavaliação

 Dentro do que estava proposto para o objetivo do projeto entendo foi possivel alcançar os resultados esperados , contudo , Por conta da fonte de dados não possuir informações que passem confiança sobre os dados apresentados não é possivel tomar decisões com base nos dados obtidos.

 As principais dificuldades encontradas durante a execução do projeto foi principalmente na etapa de estruturação do data lake , principalmente pelo fato de ser o primeiro contato com a ferramenta. Contudo, pelo fato da ferramenta possuir uma IA que apoia na idenficiação dos problemas de codigo, rapidamente foi possivel identificar as necessidades ded ajuste e correção dos dados.Também posso listar como uma oportunidade a geração das tabalas para responder as perguntas chaves, pelo fato de não ter conseguido ter confiaça sobre os dados não foi possivel validar se todos os dados estavam se comportanto conforme o esperado 

  Outro fator importante para se destacar é que como o objetivo principal estava na estruturação dos dados e não na analise propriamente dita , não foi realizada uma analise mais aprofundada com dashboard interativos e visões mais estrategicas.

  Para trabalhos futuros podemos destacar como uma oportunidade fazer o mesmo processo só que utilizando uma API para automatizar o processo de extração dos dados, como para esse trabalho existia uma registrição de recurso foi necessário utilizar dados em CSV com carga manual para reduzir a necessidade de esforço computacional .
