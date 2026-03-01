select
    [height_cm],
    [weight_kg],
    [Born_year],
    [Death_year],
    [Country],
    athletes.[athlete_id],
    results.*
from dbo.[athletes_clean] as athletes
right join dbo.[results_clean] as results on results.[athlete_id] = athletes.[athlete_id];