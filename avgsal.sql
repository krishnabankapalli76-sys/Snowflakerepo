select  avg(sal),customer_id FROM SC_FIRST.CUSTOMER 
where sal>(select avg(sal) from SC_FIRST.CUSTOMER) 
group by customer_id;