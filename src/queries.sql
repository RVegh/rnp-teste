-- Query utilizada para filtragem das tabelas relevantes para o case utilizando expressões regulares
SELECT table_name
FROM information_schema.tables
WHERE table_name !~ '^(foreign|pg)'

/* Queries utilizadas na resposta da pergunta 1: Qual país possui a maior quantidade de itens cancelados?
Parti do princípio que foi pedido o país com a maior quantidade de itens(produtos) dos pedidos com status = cancelado,
mas na dúvida, também fiz uma query retornando o país com maior quantidade de pedidos cancelados.
*/
SELECT C.COUNTRY,
       SUM(OD.QUANTITY_ORDERED) AS TOTAL_ITEMS_CANCELADOS
FROM CUSTOMERS C
LEFT JOIN ORDERS O
    ON O.CUSTOMER_NUMBER = C.CUSTOMER_NUMBER
LEFT JOIN ORDERDETAILS OD
    ON O.ORDER_NUMBER = OD.ORDER_NUMBER 
WHERE O.STATUS = 'Cancelled'
GROUP BY C.COUNTRY
ORDER BY TOTAL_ITEMS_CANCELADOS DESC
LIMIT 1;

--Uma outra opção para responder a pergunta é usar uma window function como row_number().
SELECT COUNTRY, TOTAL_ITEMS_CANCELADOS
FROM (
    SELECT C.COUNTRY,
           SUM(OD.QUANTITY_ORDERED) AS TOTAL_ITEMS_CANCELADOS,
           ROW_NUMBER() OVER (ORDER BY SUM(OD.QUANTITY_ORDERED) DESC) AS RN
    FROM CUSTOMERS C
    LEFT JOIN ORDERS O
        ON O.CUSTOMER_NUMBER = C.CUSTOMER_NUMBER
    LEFT JOIN ORDERDETAILS OD
        ON O.ORDER_NUMBER = OD.ORDER_NUMBER 
    WHERE O.STATUS = 'Cancelled'
    GROUP BY C.COUNTRY
) ranked
WHERE RN = 1;

SELECT C.COUNTRY,
	   COUNT(O.ORDER_NUMBER) AS TOTAL_PEDIDOS_CANCELADOS
FROM ORDERS O
LEFT JOIN CUSTOMERS C
ON O.CUSTOMER_NUMBER = C.CUSTOMER_NUMBER
WHERE O.STATUS = 'Cancelled'
GROUP BY C.COUNTRY
ORDER BY C.COUNTRY ASC 
LIMIT 1;

/* Pergunta 2: Qual o faturamento da linha de produto mais vendido, considere os itens com status 'Shipped', cujo o pedido foi realizado no ano de 2005? */
SELECT PD.PRODUCT_LINE,
	SUM(P.AMOUNT) AS FATURAMENTO
FROM PRODUCTS PD
LEFT JOIN ORDERDETAILS OD
ON OD.PRODUCT_CODE  = PD.PRODUCT_CODE 
LEFT JOIN ORDERS O
ON O.ORDER_NUMBER  = OD.ORDER_NUMBER 
LEFT JOIN PAYMENTS P 
ON P.CUSTOMER_NUMBER = O.CUSTOMER_NUMBER 
WHERE O.STATUS = 'Shipped'
AND EXTRACT(YEAR FROM O.ORDER_DATE) = 2005
GROUP BY PD.PRODUCT_LINE
ORDER BY FATURAMENTO DESC
LIMIT 1


/* Pergunta 3: Nome, sobrenome e email dos vendedores do Japão. O local-part do email deve estar mascarado.*/
SELECT
	E.FIRST_NAME AS NOME,
	E.LAST_NAME AS SOBRENOME,
    CONCAT(REPEAT('X', POSITION('@' IN E.EMAIL) - 1), SUBSTRING(EMAIL, POSITION('@' IN E.EMAIL))) AS EMAIL
FROM EMPLOYEES E 
LEFT JOIN OFFICES O 
ON O.OFFICE_CODE = E.OFFICE_CODE 
WHERE O.COUNTRY = 'Japan'