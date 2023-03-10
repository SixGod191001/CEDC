select first(user_aliases.name) as name,
sum(sales_aliases.amount) as total_amount
from user_aliases
inner join sales_aliases
on user_aliases.user_id = sales_aliases.user_id
where sales_aliases.amount > 0
group by name
order by total_amount