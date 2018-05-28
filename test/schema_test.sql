
select count(*) from region;
select count(*) from nation;
select count(*) from part;
select count(*) from supplier;
select count(*) from partsupp;
select count(*) from customer;
select count(*) from orders;
select count(*) from lineitem;

select avg(length(R_NAME)) from region;
select avg(length(R_COMMENT)) from region;

select avg(length(N_NAME)) from nation;
select avg(length(N_COMMENT)) from nation;

select avg(length(P_NAME)) from part;
select avg(length(P_MFGR)) from part;
select avg(length(P_BRAND)) from part;
select avg(length(P_TYPE)) from part;
select min(P_SIZE) from part;
select max(P_SIZE) from part;
select count(distinct(P_SIZE)) from part;
select avg(length(P_CONTAINER)) from part;
select min(P_RETAILPRICE) from part;
select max(P_RETAILPRICE) from part;
select avg(length(P_COMMENT)) from part;

select avg(length(S_NAME)) from supplier;
select avg(length(S_ADDRESS)) from supplier;
select avg(length(S_PHONE)) from supplier;
select min(S_ACCTBAL) from supplier;
select max(S_ACCTBAL) from supplier;
select avg(length(S_COMMENT)) from supplier;

select min(PS_AVAILQTY) from partsupp;
select max(PS_AVAILQTY) from partsupp;
select count(distinct(PS_AVAILQTY)) from partsupp;
select min(PS_SUPPLYCOST) from partsupp;
select max(PS_SUPPLYCOST) from partsupp;
select avg(length(PS_COMMENT)) from partsupp;

select avg(length(C_NAME)) from customer;
select avg(length(C_ADDRESS)) from customer;
select avg(length(C_PHONE)) from customer;
select min(C_ACCTBAL) from customer;
select max(C_ACCTBAL) from customer;
select avg(length(C_MKTSEGMENT)) from customer;
select avg(length(C_COMMENT)) from customer;

select avg(length(O_ORDERSTATUS)) from orders;
select min(O_TOTALPRICE) from orders;
select max(O_TOTALPRICE) from orders;
select min(O_ORDERDATE) from orders;
select max(O_ORDERDATE) from orders;
select avg(length(O_ORDERPRIORITY)) from orders;
select avg(length(O_CLERK)) from orders;
select min(O_SHIPPRIORITY) from orders;
select max(O_SHIPPRIORITY) from orders;
select count(distinct(O_SHIPPRIORITY)) from orders;
select avg(length(O_COMMENT)) from orders;

select min(L_QUANTITY) from lineitem;
select max(L_QUANTITY) from lineitem;
select min(L_EXTENDEDPRICE) from lineitem;
select max(L_EXTENDEDPRICE) from lineitem;
select min(L_DISCOUNT) from lineitem;
select max(L_DISCOUNT) from lineitem;
select min(L_TAX) from lineitem;
select max(L_TAX) from lineitem;
select avg(length(L_RETURNFLAG)) from lineitem;
select avg(length(L_LINESTATUS)) from lineitem;
select min(L_SHIPDATE) from lineitem;
select max(L_SHIPDATE) from lineitem;
select min(L_COMMITDATE) from lineitem;
select max(L_COMMITDATE) from lineitem;
select min(L_RECEIPTDATE) from lineitem;
select max(L_RECEIPTDATE) from lineitem;
select avg(length(L_SHIPINSTRUCT)) from lineitem;
select avg(length(L_SHIPMODE)) from lineitem;
select avg(length(L_COMMENT)) from lineitem;
