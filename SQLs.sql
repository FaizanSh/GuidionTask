select * from
(select reviewer_id,count(*) as reviews from fact.airbnb_reviews group by reviewer_id)
 order by reviews desc limit 5

select source.month, max(num_of_reviews) as review_count from
(select dates.month, count(*) num_of_reviews from fact.airbnb_reviews review
join dim.date dates
on dates.date = review.date
group by on date.month) source


SELECT 
    host_is_superhost, 
    AVG(host_response_rate) as avg_response_rate
FROM dim.host
GROUP BY host_is_superhost