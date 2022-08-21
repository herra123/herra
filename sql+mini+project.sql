/*
1.Show the percentage of wins of each bidder in the order of highest to lowest percentage.
2.Display the number of matches conducted at each stadium with stadium name, city from the database.
3.In a given stadium, what is the percentage of wins by a team which has won the toss?
4.Show the total bids along with bid team and team name.
5.Show the team id who won the match as per the win details.
6.Display total matches played, total matches won and total matches lost by team along with its team name.
7.Display the bowlers for Mumbai Indians team.
8.How many all-rounders are there in each team, Display the teams with more than 4 
all-rounder in descending order.
*/

use ipl;

#Q1.Show the percentage of wins of each bidder in the order of highest to lowest percentage.

select ibd.BIDDER_ID,ims.SCHEDULE_ID,ims.TOURNMT_ID,BID_STATUS,(count(BID_STATUS)+NO_OF_BIDS) as total_no_of_bids,
if(BID_STATUS='won',count(BID_STATUS)/(count(BID_STATUS)+NO_OF_BIDS)*100,0) as percentage_of_wins
from ipl_bidding_details ibd
join ipl_match_schedule ims
on ims.SCHEDULE_ID=ibd.SCHEDULE_ID
join ipl_bidder_points ibp
on ibp.BIDDER_ID=ibd.BIDDER_ID
where ims.TOURNMT_ID!='2018'
group by ims.TOURNMT_ID,BIDDER_ID
order by percentage_of_wins desc;

#Q2.Display the number of matches conducted at each stadium with stadium name, city from the database.

select STADIUM_NAME,CITY,ips.STADIUM_ID,count(MATCH_ID) as number_of_matches
from ipl_stadium ips
join ipl_match_schedule ims
on ims.STADIUM_ID=ips.STADIUM_ID
group by ips.STADIUM_ID;


#Q3.In a given stadium, what is the percentage of wins by a team which has won the toss?

select ipl.MATCH_ID,
if(TOSS_WINNER=MATCH_WINNER, (count(MATCH_WINNER)/MATCHES_WON)*100,0) as percentage_of_wins
from ipl_team_standings ipt
join ipl_match ipl
on ipl.TEAM_ID1=ipt.TEAM_ID
join ipl_match_schedule ims
on ims.MATCH_ID=ipl.MATCH_ID
group by ipl.MATCH_ID
order by percentage_of_wins desc;


#Q4.Show the total bids along with bid team and team name.

select ibd.BIDDER_ID,(count(BID_STATUS)+NO_OF_BIDS)as total_no_of_bids,BID_TEAM,TEAM_NAME
from ipl_bidding_details ibd
join ipl_match_schedule ims
on ims.SCHEDULE_ID=ibd.SCHEDULE_ID
join ipl_bidder_points ibp
on ibp.BIDDER_ID=ibd.BIDDER_ID
join ipl_team ipt
on ipt.TEAM_ID=ibd.BID_TEAM
where ims.TOURNMT_ID!='2018'
group by ims.TOURNMT_ID,BIDDER_ID;

#Q5.Show the team id who won the match as per the win details.


select its.team_id,t.team_name, its.total_points as 'present_year_point',lag(its.total_points,1) over (partition by its.team_id order by its.tournmt_id) as prev_year_points, 
((its.total_points-(lag(its.total_points,1) over (partition by its.team_id order by its.tournmt_id)))/(lag(its.total_points,1) over (partition by its.team_id order by its.tournmt_id)))*100 as 'win%' 
from ipl_team_standings as its inner join ipl_team as t
on its.team_id=t.team_id
order by ((its.total_points-(lag(its.total_points,1) over (partition by its.team_id order by its.tournmt_id)))/(lag(its.total_points,1) over (partition by its.team_id order by its.tournmt_id)))*100 desc 
limit 1;

#Q6.Display total matches played, total matches won and total matches lost by team along with its team name.

select it.TEAM_ID,MATCHES_PLAYED,MATCHES_WON,MATCHES_LOST,TEAM_NAME
from ipl_team_standings its
join ipl_team it
on it.TEAM_ID=its.TEAM_ID;


#Q7.Display the bowlers for Mumbai Indians team.

select it.TEAM_ID,PLAYER_ROLE,TEAM_NAME,PLAYER_NAME
from ipl_team_players itp
join ipl_team it
on it.TEAM_ID=itp.TEAM_ID
join ipl_player ip
on ip.PLAYER_ID=itp.PLAYER_ID
where TEAM_NAME like '%Mumbai Indians%' and PLAYER_ROLE like '%Bowler%';

#Q8.How many all-rounders are there in each team, Display the teams with more than 4 all-rounder in descending order.

select ip.TEAM_ID,ipl.PLAYER_ID,TEAM_NAME,PLAYER_NAME,PLAYER_ROLE,count(PLAYER_ROLE) as number_of_all_rounders
from ipl_team_players itp
join ipl_team ip
on ip.TEAM_ID=itp.TEAM_ID
join ipl_player ipl
on ipl.PLAYER_ID=itp.PLAYER_ID
where PLAYER_ROLE like '%All-Rounder%'
group by ip.TEAM_ID
having count(PLAYER_ROLE)>4;
