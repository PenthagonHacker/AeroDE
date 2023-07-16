CREATE OR REPLACE FUNCTION aerode.f_parse_statistics()
 RETURNS integer
 LANGUAGE plpgsql
AS $function$

begin

--// parsing statsSingleSeason

insert into aerode.statssingleseason 
select
	displayName,
	id,
	description,
	postseason,
   (splits -> 'stat' ->> 'gamesPlayed')::int gamesPlayed,
   (splits -> 'stat' ->> 'wins')::int wins,
   (splits -> 'stat' ->> 'losses')::int losses,
   (splits -> 'stat' ->> 'ot')::int ot,
   (splits -> 'stat' ->> 'pts')::int  pts,
   (splits -> 'stat' ->> 'goalsPerGame')::float goalsPerGame,
   (splits -> 'stat' ->> 'goalsAgainstPerGame')::float goalsAgainstPerGame,
   (splits -> 'stat' ->> 'evGGARatio')::float evGGARatio,
   (splits -> 'stat' ->> 'powerPlayPercentage')::float powerPlayPercentage,
   (splits -> 'stat' ->> 'powerPlayGoals')::float powerPlayGoals,
   (splits -> 'stat' ->> 'powerPlayGoalsAgainst')::float powerPlayGoalsAgainst,
   (splits -> 'stat' ->> 'powerPlayOpportunities')::float powerPlayOpportunities,  
   (splits -> 'stat' ->> 'penaltyKillPercentage')::float penaltyKillPercentage,
   (splits -> 'stat' ->> 'shotsPerGame')::float shotsPerGame,   
   (splits -> 'stat' ->> 'shotsAllowed')::float shotsAllowed,
   (splits -> 'stat' ->> 'winScoreFirst')::float winScoreFirst,
   (splits -> 'stat' ->> 'winOppScoreFirst')::float winOppScoreFirst,
   (splits -> 'stat' ->> 'winLeadFirstPer')::float winLeadFirstPer,
   (splits -> 'stat' ->> 'winLeadSecondPer')::float winLeadSecondPer,
   (splits -> 'stat' ->> 'winOutshootOpp')::float winOutshootOpp,
   (splits -> 'stat' ->> 'winOutshotByOpp')::float winOutshotByOpp,
   (splits -> 'stat' ->> 'faceOffsTaken')::float faceOffsTaken,
   (splits -> 'stat' ->> 'faceOffsWon')::float faceOffsWon,
   (splits -> 'stat' ->> 'faceOffsLost')::float faceOffsLost,
   (splits -> 'stat' ->> 'faceOffWinPercentage')::float faceOffWinPercentage,
   (splits -> 'stat' ->> 'shootingPctg')::float  shootingPctg,
   (splits -> 'stat' ->> 'savePctg')::float savePctg,
   (splits -> 'team'->>'id')::int teamid,
   (splits -> 'team'->>'name')::text "teamname",
   (splits -> 'team'->>'link')::text teamlink
from
	(
	select
		(stats -> 'type' ->> 'displayName')::text displayName,
		(stats -> 'type' -> 'gameType'->>'id')::text id,
		(stats -> 'type' -> 'gameType'->>'description')::text description,
		(stats -> 'type' -> 'gameType'->>'postseason')::bool postseason,
		jsonb_array_elements(stats -> 'splits') splits
	from
		(
		select
			jsonb_array_elements(jsn::jsonb->'stats')::jsonb stats
		from
			aerode.stage_json n) q) q1
	where displayName = 'statsSingleSeason';
	
	
	
--// parsing regularSeasonStatRankings
	
insert into aerode.regularseasonstatrankings
select
	displayName,
   (splits -> 'stat' ->> 'wins')::text wins,
   (splits -> 'stat' ->> 'losses')::text losses,
   (splits -> 'stat' ->> 'ot')::text ot,
   (splits -> 'stat' ->> 'pts')::text  pts,
   (splits -> 'stat' ->> 'goalsPerGame')::text goalsPerGame,
   (splits -> 'stat' ->> 'goalsAgainstPerGame')::text goalsAgainstPerGame,
   (splits -> 'stat' ->> 'evGGARatio')::text evGGARatio,
   (splits -> 'stat' ->> 'powerPlayPercentage')::text powerPlayPercentage,
   (splits -> 'stat' ->> 'powerPlayGoals')::text powerPlayGoals,
   (splits -> 'stat' ->> 'powerPlayGoalsAgainst')::text powerPlayGoalsAgainst,
   (splits -> 'stat' ->> 'powerPlayOpportunities')::text powerPlayOpportunities,  
   (splits -> 'stat' ->> 'penaltyKillPercentage')::text penaltyKillPercentage,
   (splits -> 'stat' ->> 'shotsPerGame')::text shotsPerGame,   
   (splits -> 'stat' ->> 'shotsAllowed')::text shotsAllowed,
   (splits -> 'stat' ->> 'winScoreFirst')::text winScoreFirst,
   (splits -> 'stat' ->> 'winOppScoreFirst')::text winOppScoreFirst,
   (splits -> 'stat' ->> 'winLeadFirstPer')::text winLeadFirstPer,
   (splits -> 'stat' ->> 'winLeadSecondPer')::text winLeadSecondPer,
   (splits -> 'stat' ->> 'winOutshootOpp')::text winOutshootOpp,
   (splits -> 'stat' ->> 'winOutshotByOpp')::text winOutshotByOpp,
   (splits -> 'stat' ->> 'faceOffsTaken')::text faceOffsTaken,
   (splits -> 'stat' ->> 'faceOffsWon')::text faceOffsWon,
   (splits -> 'stat' ->> 'faceOffsLost')::text faceOffsLost,
   (splits -> 'stat' ->> 'faceOffWinPercentage')::text faceOffWinPercentage,
   (splits -> 'stat' ->> 'shootingPctRank')::text shootingPctRank,
   (splits -> 'stat' ->> 'savePctRank')::text savePctRank,
   (splits -> 'team'->>'id')::text teamid,
   (splits -> 'team'->>'name')::text "teamname",
   (splits -> 'team'->>'link')::text teamlink
from
	(
	select
		(stats -> 'type' ->> 'displayName')::text displayName,
		(stats -> 'type' ->> 'gameType')::text gameType, 
		jsonb_array_elements(stats -> 'splits') splits
	from
		(
		select
			jsonb_array_elements(jsn::jsonb->'stats')::jsonb stats
		from
			aerode.stage_json n) q) q1
	where displayName = 'regularSeasonStatRankings';

return 1;

exception 
	when others then
return 0;


end;
$function$
;