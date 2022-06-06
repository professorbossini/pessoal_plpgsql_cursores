DROP TABLE tb_top_youtubers;
CREATE TABLE tb_top_youtubers(
    cod_top_youtubers SERIAL PRIMARY KEY,
    rank INT,
    youtuber VARCHAR(200),
    subscribers INT,
    video_views VARCHAR(200),
    video_count INT,
    category VARCHAR(200),
    started INT
);

SELECT * FROM tb_top_youtubers;
DELETE FROM tb_top_youtubers;


DO $$
DECLARE
    --1. declaração do cursor
    --esse cursor é unbound por não ser associado a nenhuma query
    cur_nomes_youtubers REFCURSOR;
    --para armazenar o nome do youtuber a cada iteração
    v_youtuber VARCHAR(200);
BEGIN
    --2. abertura do cursor
    OPEN cur_nomes_youtubers FOR 
        SELECT youtuber 
            FROM
        tb_top_youtubers;
        
    LOOP
        --3. Recuperação dos dados de interesse
        FETCH cur_nomes_youtubers INTO v_youtuber;
        --FOUND é uma variável especial que indica
        EXIT WHEN NOT FOUND;
        RAISE NOTICE '%', v_youtuber;
        
    END LOOP;
    --4. Fechamento do cursos
    CLOSE cur_nomes_youtubers;
END;
$$


DO $$
DECLARE
    cur_nomes_a_partir_de REFCURSOR;
    v_youtuber VARCHAR(200);
    v_ano INT := 2008;
    v_nome_tabela VARCHAR(200) := 'tb_top_youtubers';
BEGIN
    OPEN cur_nomes_a_partir_de FOR EXECUTE
        format 
        (
            '
            SELECT 
                youtuber 
            FROM
                %s
            WHERE started >= $1
            '
            ,
           v_nome_tabela
        )USING v_ano;
    LOOP
        
        FETCH cur_nomes_a_partir_de INTO v_youtuber;
        EXIT WHEN NOT FOUND;
        RAISE NOTICE '%', v_youtuber;        
    END LOOP;
    CLOSE cur_nomes_a_partir_de;
END;
$$


DO $$
DECLARE
    --cursor vinculado (bound)
    cur_nomes_e_inscritos CURSOR FOR SELECT youtuber, subscribers FROM tb_top_youtubers;
    --capaz de abrigar uma tupla inteira
    --tupla.youtuber nos dá o nome do youtuber
    --tupla.subscribers nos dá o número de inscritos
    tupla RECORD;
    resultado TEXT DEFAULT '';
BEGIN
    OPEN cur_nomes_e_inscritos;
    FETCH cur_nomes_e_inscritos INTO tupla;
    WHILE FOUND LOOP
        resultado := resultado || tupla.youtuber || ':' || tupla.subscribers  || ',';
        FETCH cur_nomes_e_inscritos INTO tupla;
    END LOOP;
    CLOSE cur_nomes_e_inscritos;
    RAISE NOTICE '%', resultado;
END;

$$


DO $$

DECLARE
    v_ano INT := 2010;
    v_inscritos INT := 60_000_000;
    cur_ano_inscritos CURSOR (ano INT, inscritos INT) FOR SELECT youtuber FROM tb_top_youtubers WHERE started >= ano AND subscribers >= inscritos;
    v_youtuber VARCHAR(200);
BEGIN
    --execute apenas um dos dois comandos OPEN a seguir
    -- passando argumentos pela ordem
   OPEN cur_ano_inscritos (v_ano, v_inscritos);
    --passando argumentos por nome
   OPEN cur_ano_inscritos (inscritos := v_inscritos, ano := v_ano);
    LOOP
        FETCH cur_ano_inscritos INTO v_youtuber;
        EXIT WHEN NOT FOUND;
        RAISE NOTICE '%', v_youtuber;
    END LOOP;
    CLOSE cur_ano_inscritos;
END;
$$

CREATE OR REPLACE FUNCTION fn_subscribers ()
RETURNS refcursor
AS $$
DECLARE
    meu_cursor REFCURSOR;
BEGIN
    OPEN meu_cursor FOR SELECT youtuber, subscribers FROM tb_top_youtubers WHERE rank <= 5;
    RETURN meu_cursor;

END;
$$
LANGUAGE plpgsql;
    
DO $$
BEGIN
    PERFORM fn_subscribers();
    
END;
$$

