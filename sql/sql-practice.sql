
-- 데이터 검색 기본 쿼리 연습
-- 1. 전체 테이블 데이터 조회
select * from product;
select * from seller;
select * from ordered;

-- 2. 데이터를 조회할 때 특정 칼럼만 출력 
select name, price from product;

-- 3. 함수를 적용하거나 수시을 적용하여 출력할 때 칼럼값 변경 
select name, price / 1000 from product;
select name, concat(price / 1000, '천원') from product;

-- 4. 조건을 만족하는 데이터만 출력 
select * from product where price > 50000; -- 가격이 5만원 이상인 제품만
select * from product where price between 50000 and 100000; -- 5만원에서 10만원 사
select * from product where name like '%테이블%'; -- 제품명에 테이블이 들어가는 제품
	
-- 5. 가격 낮은 순으로 정렬
select * from product order by price asc;

-- 6. 가격 높은 순으로 정렬 
select * from product order by price desc;
	
-- 7. 정렬 칼럼이 여러 개 
select * from product order by price asc, name desc

-- 8. 결과 수 제한 
select * from product limit 3

-- 9. 정렬 후 결과 수 제한 
select * from product order by price limit 3

-- 10. 집계함수
select count(*) from product; -- 총 상품 갯수 
select sum(num_stock) from product; -- 총 재고 수

-- 11. 상품 판매자별 총 재고 상품 갯수
select seller, sum(num_stock) from product group by seller

-- 12. 상품과 판매자명
select p.* from product p;
select p.*, s.* from product     p join seller s on p.seller = s.id;
select p.name, p.price, s.name, p.num_stock from product p join seller s on p.seller = s.id;

-- 13. 실습 과제
-- 2023-10-03에 주문된 상품(product)의 갯수를 검색하는 쿼리를 짜세요. 상품명별로 주문된 총 갯수 나와야합니다. 아래와 같은 결과가 나와야 합니다.
-- Hint: Join, Where, Group by, Sum을 이용하세요
-- Hint: 10월 3일에 주문된 상품을 가져오기 위해서는 어떤 칼럼이 3일 0시보다는 크거나 작고 4일보다는 작은거겠죠?
A사 친환경 테이블     | 10
B사 접이식 캠핑 테이블 | 3
E사 에어프라이어      | 1




select sum(o.amount), p.name from ordered o join product p on o.product_id = p.id where order_date >= '2023-10-03' and order_date < '2023-10-04'
group by p.name

