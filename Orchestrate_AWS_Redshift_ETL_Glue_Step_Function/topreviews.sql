topreviews.sql
----------------
UNLOAD ('SELECT marketplace, product_category, product_title, review_id, helpful_votes, AVG(star_rating) as average_stars FROM public.reviews GROUP BY marketplace, product_category, product_title, review_id, helpful_votes ORDER BY helpful_votes DESC, average_stars DESC')
TO 's3://redshift-databucket-output2023/unload/'
iam_role 'arn:aws:iam::398401345267:role/stack2-RedshiftRole-1K7GSTEF4DAVM’;
