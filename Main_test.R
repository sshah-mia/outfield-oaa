### this is going to be the main test file

#### Center Field 

require(RODBC)
require(dplyr)
require(mgcv)
require(odbc)
require(xgboost)
require(DBI)

is.daily.load <- FALSE # simply setting this flag to TRUE will cause all appropriate
# bits and filters to be set up for the daily load

# To run this script, simply change the dates below to the desired date range
# and Ctrl+A+Enter. If running the script to evaluate just one day of OAA,
# make sure the start date and end date are the same.
# Make sure to run this script after the infield script
eval.start.date <- "2024-01-01" #as.character(Sys.Date() - 1) 
eval.end.date <- "2025-01-01"
#eval.end.date <- as.character(Sys.Date() - 1) 

flipFlags <- FALSE
writeToDB <- TRUE
default.in.postgame.report <- TRUE

look.for.deflections <- FALSE
postgame.report.dates <- as.character(Sys.Date() - 2)



setwd("//marlins.pvt/FileShares/GroupShares/BaseballOperations_Pvt/Analytics/SShah/OAA/Catch Probability Improvement")

append.results <- TRUE # whether or not to append the newly evaluated data (if false, will overwrite table)
recreate.data <- TRUE # whether or not to pull the data from the dates listed,
# or to just use the data available in scratch tables

min.reasonable.of.start.dist <- 235
min.outfielder.responsibility <- 200 # anything beyond this is a ball the outfielder will play, not infielder

max.infielder.responsibility <- 190 # anything above this number is a ball the infielder does not go for - pop ups are NOT included
min.popup.angle <- 15 # balls less than max.infielder.responsibility distance and over this angle are excluded
max.reasonable.start.dist <- 240 
max.reasonable.of.start.dist <- 375

distance.to.be.under.ball <- 15 # if a fielder is within this many feet of ball's landing spot, he is "under" the ball
distance.to.not.be.responsible <- 30 # if a fielder is this many feet from the ball's landing spot and another fielder is
# under the ball, he will not be evaluated

single.erv <- .449
double.erv <- .755
triple.erv <- 1.078
out.erv <- -.260

# The finding_deflected_balls script contains a function that will 
# find deflected balls from the data passed to it, using the API
#source("finding_deflected_balls.R")


# Grading players on catch probability metrics - actual start positions first

lagoon <- odbcConnect("Coral")
if (recreate.data == FALSE) {
  data <- readRDS("data_for_catch_probability_model.RDS")
} else {
  data <- sqlQuery(lagoon, paste0("select ppv.gameDate, ppv.mlbamGameID, gm.ID as lagoonGameID, seqID, inning, inningHalf,
                                  ballsBefore, strikesBefore, outsBefore, pitcher, batter, throws, bats, 
                                             batterMlbamPlayerID as batterID, pitcherMlbamPlayerID as pitcherID, pitchType,
                                             playResult, hitType, fieldedByPos, OutsRecorded, fieldedByMLBAMID as fielderID, levelAbbr,
                                             isRunnerEndOn1st, isRunnerEndOn2nd, isRunnerEndOn3rd, B1, B2, B3, HR, isGB, isBunt, out,
                                            -- case when playDesc like '%inside-the-park home run%' then 1 else 0 end as isInsideParkHR,
                                             playerOut1stMlbamID, playerOut2ndMlbamID, playerOut3rdMlbamID, isError, errorPlayerMlbamID,
                                             distance, bearing, hangtime, exitSpeed, angle,
                                             direction, isCaught, ppv.pitcherTeamAbbr,
                                             fielder1B_startAngle, fielder1B_startDist, fielder1B_ThrowVelo, fielder1B_ThrowBase,
                                             fielder2B_startAngle, fielder2B_startDist, fielder2B_ThrowVelo, fielder2B_ThrowBase,
                                             fielderSS_startAngle, fielderSS_startDist, fielderSS_ThrowVelo, fielderSS_ThrowBase,
                                             fielder3B_startAngle, fielder3B_startDist, fielder3B_ThrowVelo, fielder3B_ThrowBase,
                                             fielderLF_startAngle, fielderLF_startDist, fielderLF_ThrowVelo, fielderLF_ThrowBase,
                                             fielderCF_startAngle, fielderCF_startDist, fielderCF_ThrowVelo, fielderCF_ThrowBase,
                                             fielderRF_startAngle, fielderRF_startDist, fielderRF_ThrowVelo, fielderRF_ThrowBase,
                                             fielderP_ThrowBase, fielderP_ThrowVelo,
                                             mlbamPlayer1BId, mlbamPlayer2BId, mlbamPlayerSSId, mlbamPlayer3BId, mlbamPlayerLFId,
                                             mlbamPlayerCFId, mlbamPlayerRFId, ballLandingPosX, ballLandingPosY,
                                             playDesc, isRunnerOn1st, isRunnerOn2nd, isRunnerOn3rd,
                                             ProjectedDistanceOfBallFromFence_Ft as battedBallDistanceFromFence,
                                             HitSpinRate_RPM as HitSpinRate,
                                             HitSpinAngle,
                                             year(ppv.gamedate) as year,
                                              ppv.venueName, vn.mlbamVenueID,
                                              ppv.batTeamScore, ppv.pitTeamScore
                                             from lagoon.event.pro_plays_statcast_vw ppv
                                             JOIN lagoon.base.pro_games gm
                                             ON ppv.mlbamGameID = gm.mlbamGameID
                                               JOIN lagoon.base.pro_venues vn
                                               ON gm.venueID = vn.ID
                                             where ppv.gameDate  <= '", eval.end.date,"' and ppv.gameDate >= '", eval.start.date, "'
                                                and CI = 0 and gm.levelid <= 5 and (ppv.altLevelAbbr in ('mlb') or mlbamTrackingVersionId in (103, 112, 114))
                                             order by seqID"), stringsAsFactors = FALSE) %>% as_tibble()
  
  
  
  ### SUMAIR TIM UPDATE 6/28/24 -- FOR MiLB Plays we are replacing ball tracking with fielder tracking on plays with potential ball tracking issues.
  sqlQuery(lagoon,paste0('drop table if exists #TempPreprocessedData;'))  
  
  
  
  data_to_join <- sqlQuery(lagoon, paste0("
  drop table if exists #TempPreprocessedData;
  WITH PreprocessedData AS (
      SELECT
          CONCAT('https://research.mlb.com/games/', p.mlbamGameID, '/plays/', p.guid) AS url,
          p.gamedate,
          p.mlbamgameId,
          p.seqId,
          p.guid,
          g.fieldedByPos,
          p.fielderDistance,
          g.distance AS originalHitDistance,
          p.fielderBearing,
          g.bearing AS hitBearing,
          g.hangtime,
          g.isCaught,
          g.Angle,
          ballLandingPosX,
          ballLandingPosY,
          CASE g.fieldedByPos
              WHEN 'LF' THEN FielderLF_StartDist
              WHEN 'CF' THEN FielderCF_StartDist
              WHEN 'RF' THEN FielderRF_StartDist
              WHEN '1B' THEN Fielder1B_StartDist
              WHEN '2B' THEN Fielder2B_StartDist
              WHEN '3B' THEN Fielder3B_StartDist
              WHEN 'SS' THEN FielderSS_StartDist
              ELSE NULL
          END AS FielderStartPos,
          CASE
              WHEN g.fieldedByPos IN ('LF', 'CF', 'RF', '1B', '2B', '3B', 'SS') AND 
                  (ABS((CASE g.fieldedByPos
                      WHEN 'LF' THEN FielderLF_StartDist
                      WHEN 'CF' THEN FielderCF_StartDist
                      WHEN 'RF' THEN FielderRF_StartDist
                      WHEN '1B' THEN Fielder1B_StartDist
                      WHEN '2B' THEN Fielder2B_StartDist
                      WHEN '3B' THEN Fielder3B_StartDist
                      WHEN 'SS' THEN FielderSS_StartDist
                      ELSE 0 
                  END) - p.fielderDistance) / g.hangtime > 22) THEN
                  g.distance -- Set to original hit distance
              WHEN g.fieldedByPos IN ('LF', 'CF', 'RF', '1B', '2B', '3B', 'SS') AND 
                  (ABS((CASE g.fieldedByPos
                      WHEN 'LF' THEN FielderLF_StartDist
                      WHEN 'CF' THEN FielderCF_StartDist
                      WHEN 'RF' THEN FielderRF_StartDist
                      WHEN '1B' THEN Fielder1B_StartDist
                      WHEN '2B' THEN Fielder2B_StartDist
                      WHEN '3B' THEN Fielder3B_StartDist
                      WHEN 'SS' THEN FielderSS_StartDist
                      ELSE 0 
                  END) - p.fielderDistance) / g.hangtime <= 22) AND 
                  ABS(p.fielderDistance - g.distance) > 12 THEN
                  p.fielderDistance + 1 -- Adjust the hit distance
              ELSE
                  g.distance -- Default to original hit distance
          END AS modifiedHitDistance
      FROM lagoon.event.pro_plays_statcast_vw g
      JOIN Scratch.pugh.oaaOutfieldDataErrorsRawData p
          ON p.gameDate = g.gameDate AND p.mlbamgameId = g.mlbamgameId AND p.guid = g.guid
      WHERE g.isCaught = 1 AND LevelAbbr != 'MLB' and g.gameDate  <= '", eval.end.date,"' and g.gameDate >= '", eval.start.date, "'
      
  )
  SELECT *,
         modifiedHitDistance * SIN(hitBearing * PI() / 180) + 5 AS ballLandingX,
         modifiedHitDistance * COS(hitBearing * PI() / 180) + 5 AS ballLandingY
  FROM PreprocessedData;"), stringsAsFactors = FALSE) %>% as_tibble() 
  
  # Prepare data_to_join_X with modified column names and conditional new_bearing
  data_to_join_X <- data_to_join %>%
    rename_with(~ paste0(., "_join")) %>%
    mutate(new_bearing = if_else(modifiedHitDistance_join != originalHitDistance_join, 
                                 fielderBearing_join, 
                                 hitBearing_join))
  
  # Join data_to_join_X with the main dataframe and apply conditional transformations
  data <- data %>%
    left_join(data_to_join_X, by = c("gameDate" = "gamedate_join", "mlbamGameID" = "mlbamgameId_join", "seqID" = "seqId_join")) %>%
    mutate(
      distance = if_else(!is.na(modifiedHitDistance_join) & modifiedHitDistance_join != originalHitDistance_join, 
                         modifiedHitDistance_join, 
                         distance),
      bearing = if_else(!is.na(modifiedHitDistance_join) & modifiedHitDistance_join != originalHitDistance_join, 
                        new_bearing, 
                        bearing),
      ballLandingPosX = if_else(!is.na(modifiedHitDistance_join) & modifiedHitDistance_join != originalHitDistance_join, 
                                distance * sin(bearing * pi / 180), 
                                ballLandingPosX),
      ballLandingPosY = if_else(!is.na(modifiedHitDistance_join) & modifiedHitDistance_join != originalHitDistance_join, 
                                distance * cos(bearing * pi / 180), 
                                ballLandingPosY),
      ballLandingX = if_else(!is.na(modifiedHitDistance_join) & modifiedHitDistance_join != originalHitDistance_join, 
                             ballLandingPosX, 
                             ballLandingPosX),
      ballLandingY = if_else(!is.na(modifiedHitDistance_join) & modifiedHitDistance_join != originalHitDistance_join, 
                             ballLandingPosY, 
                             ballLandingPosY),
      bearing = if_else(!is.na(modifiedHitDistance_join) & modifiedHitDistance_join != originalHitDistance_join, 
                        atan2(ballLandingX, ballLandingY) * 180 / pi, 
                        bearing),
      distance_new = if_else(!is.na(modifiedHitDistance_join) & modifiedHitDistance_join != originalHitDistance_join, 
                             sqrt(ballLandingX^2 + ballLandingY^2), 
                             distance) 
    )
  
  # Fixing data issues with specific plays
  data <- data %>%
    mutate(fielderLF_startAngle = ifelse(mlbamGameID == 661978 & seqID == 15, -25.13, fielderLF_startAngle),
           fielderCF_startAngle = ifelse(mlbamGameID == 661978 & seqID == 15, -3.75, fielderCF_startAngle),
           fielderRF_startAngle = ifelse(mlbamGameID == 661978 & seqID == 15, 22.67, fielderRF_startAngle),
           fielder1B_startAngle = ifelse(mlbamGameID == 661978 & seqID == 15, 38.38, fielder1B_startAngle),
           fielder2B_startAngle = ifelse(mlbamGameID == 661978 & seqID == 15, 19.48, fielder2B_startAngle),
           fielderSS_startAngle = ifelse(mlbamGameID == 661978 & seqID == 15, -2.0, fielderSS_startAngle),
           fielder3B_startAngle = ifelse(mlbamGameID == 661978 & seqID == 15, -25.6, fielder3B_startAngle))
  
  data <- data %>%
    mutate(fielderLF_startAngle = ifelse(mlbamGameID == 662490 & seqID == 227,  -27.5, fielderLF_startAngle),
           fielderCF_startAngle = ifelse(mlbamGameID == 662490 & seqID == 227, 1.05, fielderCF_startAngle),
           fielderRF_startAngle = ifelse(mlbamGameID == 662490 & seqID == 227, 28.3, fielderRF_startAngle),
           fielder1B_startAngle = ifelse(mlbamGameID == 662490 & seqID == 227, 25.7, fielder1B_startAngle),
           fielder2B_startAngle = ifelse(mlbamGameID == 662490 & seqID == 227, 8.32, fielder2B_startAngle),
           fielderSS_startAngle = ifelse(mlbamGameID == 662490 & seqID == 227, -14.9, fielderSS_startAngle),
           fielder3B_startAngle = ifelse(mlbamGameID == 662490 & seqID == 227, -27.5, fielder3B_startAngle))
  
  data <- data %>%
    mutate(battedBallDistanceFromFence = ifelse(mlbamGameID == 662462 & seqID == 133, -2, battedBallDistanceFromFence)) %>%
    mutate(battedBallDistanceFromFence = ifelse(mlbamGameID == 717488 & seqID == 170, -6, battedBallDistanceFromFence)) %>%
    mutate(battedBallDistanceFromFence = ifelse(mlbamGameID == 717047 & seqID == 43, 4, battedBallDistanceFromFence)) 
  
  
  data <- data %>%
    mutate(distance = ifelse(mlbamGameID == 661417 & seqID == 241, 375, distance),
           battedBallDistanceFromFence = ifelse(mlbamGameID == 661417 & seqID == 241, -2, battedBallDistanceFromFence),
           ballLandingPosX = ifelse(mlbamGameID == 661417 & seqID == 241, sin(bearing * pi/180) * distance, ballLandingPosX),
           ballLandingPosY = ifelse(mlbamGameID == 661417 & seqID == 241, cos(bearing * pi/180) * distance, ballLandingPosY)) 
  
  data <- data %>%
    mutate(distance = ifelse(mlbamGameID == 662260 & seqID == 168, 333, distance),
           battedBallDistanceFromFence = ifelse(mlbamGameID == 662260 & seqID == 168, -7, battedBallDistanceFromFence),
           ballLandingPosX = ifelse(mlbamGameID == 662260 & seqID == 168, sin(bearing * pi/180) * distance, ballLandingPosX),
           ballLandingPosY = ifelse(mlbamGameID == 662260 & seqID == 168, cos(bearing * pi/180) * distance, ballLandingPosY)) 
  
  data <- data %>%
    mutate(distance = ifelse(mlbamGameID == 662534 & seqID == 160, 401, distance),
           battedBallDistanceFromFence = ifelse(mlbamGameID == 662534 & seqID == 160, 4, battedBallDistanceFromFence),
           ballLandingPosX = ifelse(mlbamGameID == 662534 & seqID == 160, sin(bearing * pi/180) * distance, ballLandingPosX),
           ballLandingPosY = ifelse(mlbamGameID == 662534 & seqID == 160, cos(bearing * pi/180) * distance, ballLandingPosY))
  
  data <- data %>%
    mutate(battedBallDistanceFromFence = ifelse(mlbamGameID == 717759  & seqID == 336, 4, battedBallDistanceFromFence))
  
  
  end.player.positions <- sqlQuery(lagoon, paste0("select endft.date as gameDate, endft.gameId,
                                                  endft.seqId, endft.position,
                                                  endft.valueNumeric as fielder_endDist,
                                                  endang.valueNumeric as fielder_endAngle
                                                  from lagoon.stat.Pro_Major_BAM_Statcast_Prod endft
                                                  join lagoon.stat.Pro_Major_BAM_Statcast_Prod endang
                                                  on endft.date = endang.date and endft.gameId = endang.gameId
                                                  and endft.seqId = endang.seqId
                                                  and endft.position = endang.position
                                                  where endft.metricName = 61 and endang.metricName = 68
                                                    and endft.date >= '", eval.start.date, "' and endft.date <= '", eval.end.date, "'
                                                  
                                                  UNION ALL
                                                  
                                                  select endft.date as gameDate, endft.gameId,
                                                  endft.seqId, endft.position,
                                                  endft.valueNumeric as fielder_endDist,
                                                  endang.valueNumeric as fielder_endAngle
                                                  from lagoon.stat.Pro_Minor_BAM_Statcast_Prod endft
                                                  join lagoon.stat.Pro_Minor_BAM_Statcast_Prod endang
                                                  on endft.date = endang.date and endft.gameId = endang.gameId
                                                  and endft.seqId = endang.seqId
                                                  and endft.position = endang.position
                                                  where endft.metricName = 61 and endang.metricName = 68
                                                    and endft.date >= '", eval.start.date, "' and endft.date <= '", eval.end.date, "'"),
                                   stringsAsFactors = FALSE) %>% as_tibble()
  
  batter.sprint.speed <- sqlQuery(lagoon,"Select * from scratch.gahart.baseball_savant_sprint_speed",
                                  stringsAsFactors = FALSE) %>% as_tibble()
  
  run.groups <- sqlQuery(lagoon, "select * from coral.dbo.Run_Groups_TOPS", stringsAsFactors = FALSE) %>%
    as_tibble()
  
  data <- data %>% left_join(batter.sprint.speed %>% 
                               dplyr::select(batterID = player_id, batterSprintSpeed = sprint_speed, year, batterPosition = position),
                             by = c("batterID", "year")) %>%
    left_join(run.groups, by = c("batterID" = "mlbamplayerid")) %>%
    left_join(end.player.positions %>% dplyr::filter(position == "1B") %>% 
                dplyr::select(gameDate, lagoonGameID = gameId, seqID = seqId, 
                              fielder1B_endDist = fielder_endDist, fielder1B_endAngle = fielder_endAngle),
              by = c("gameDate", "lagoonGameID", "seqID")) %>%
    left_join(end.player.positions %>% dplyr::filter(position == "2B") %>% 
                dplyr::select(gameDate, lagoonGameID = gameId, seqID = seqId, 
                              fielder2B_endDist = fielder_endDist, fielder2B_endAngle = fielder_endAngle),
              by = c("gameDate", "lagoonGameID", "seqID")) %>%
    left_join(end.player.positions %>% dplyr::filter(position == "SS") %>% 
                dplyr::select(gameDate, lagoonGameID = gameId, seqID = seqId, 
                              fielderSS_endDist = fielder_endDist, fielderSS_endAngle = fielder_endAngle),
              by = c("gameDate", "lagoonGameID", "seqID")) %>%
    left_join(end.player.positions %>% dplyr::filter(position == "3B") %>% 
                dplyr::select(gameDate, lagoonGameID = gameId, seqID = seqId, 
                              fielder3B_endDist = fielder_endDist, fielder3B_endAngle = fielder_endAngle),
              by = c("gameDate", "lagoonGameID", "seqID")) %>%
    left_join(end.player.positions %>% dplyr::filter(position == "LF") %>% 
                dplyr::select(gameDate, lagoonGameID = gameId, seqID = seqId, 
                              fielderLF_endDist = fielder_endDist, fielderLF_endAngle = fielder_endAngle),
              by = c("gameDate", "lagoonGameID", "seqID")) %>%
    left_join(end.player.positions %>% dplyr::filter(position == "CF") %>% 
                dplyr::select(gameDate, lagoonGameID = gameId, seqID = seqId, 
                              fielderCF_endDist = fielder_endDist, fielderCF_endAngle = fielder_endAngle),
              by = c("gameDate", "lagoonGameID", "seqID")) %>%
    left_join(end.player.positions %>% dplyr::filter(position == "RF") %>% 
                dplyr::select(gameDate, lagoonGameID = gameId, seqID = seqId, 
                              fielderRF_endDist = fielder_endDist, fielderRF_endAngle = fielder_endAngle),
              by = c("gameDate", "lagoonGameID", "seqID")) 
  
  # We will filter out all plays that occur in bottom of last inning when GW run will score from 3rd
  # regardless of whether or not fielder catches the ball
  data <- data %>%
    dplyr::filter(!(inning >= 9 & inningHalf == "Bottom" & outsBefore < 2 & isRunnerOn3rd == 1
                    & batTeamScore == pitTeamScore))
  
  # Add in the position of the error player
  data <- data %>%
    mutate(errorPosition = ifelse(isError == 0, NA,
                                  ifelse(errorPlayerMlbamID == mlbamPlayer1BId, "1B",
                                         ifelse(errorPlayerMlbamID == mlbamPlayer2BId, "2B",
                                                ifelse(errorPlayerMlbamID == mlbamPlayerSSId, "SS",
                                                       ifelse(errorPlayerMlbamID == mlbamPlayer3BId, "3B",
                                                              ifelse(errorPlayerMlbamID == mlbamPlayerLFId, "LF",
                                                                     ifelse(errorPlayerMlbamID == mlbamPlayerCFId, "CF",
                                                                            ifelse(errorPlayerMlbamID == mlbamPlayerRFId, "RF", NA)))))))))
  
  players <- sqlQuery(lagoon, "SELECT mlbamID, throws, nameFull from lagoon.base.players", stringsAsFactors = FALSE) %>%
    as_tibble()
  
  tops.clusters <- sqlQuery(lagoon, paste0("SELECT mlbamGameID, seqID, firstMaxClust as TOPSCluster 
                          FROM [Coral].[dbo].[ml_trackman_eRV]
                          where gameDate >= '", eval.start.date, "' and gameDate <= '", eval.end.date, "'"), stringsAsFactors = FALSE) %>%
    as_tibble()
  
  data <- data %>%
    left_join(players %>% dplyr::select(mlbamPlayer1BId = mlbamID, throws1B = throws), by = "mlbamPlayer1BId") %>%
    left_join(players %>% dplyr::select(mlbamPlayer2BId = mlbamID, throws2B = throws), by = "mlbamPlayer2BId") %>%
    left_join(players %>% dplyr::select(mlbamPlayer3BId = mlbamID, throws3B = throws), by = "mlbamPlayer3BId") %>%
    left_join(players %>% dplyr::select(mlbamPlayerSSId = mlbamID, throwsSS = throws), by = "mlbamPlayerSSId") %>%
    left_join(players %>% dplyr::select(mlbamPlayerLFId = mlbamID, throwsLF = throws), by = "mlbamPlayerLFId") %>%
    left_join(players %>% dplyr::select(mlbamPlayerCFId = mlbamID, throwsCF = throws), by = "mlbamPlayerCFId") %>%
    left_join(players %>% dplyr::select(mlbamPlayerRFId = mlbamID, throwsRF = throws), by = "mlbamPlayerRFId") %>%
    left_join(tops.clusters, by = c("mlbamGameID", "seqID"))
  
  ## TIM UPDATE 6/16/24. MiLB data has a lot more player tagging issues. We are only filtering out MLB plays here. For MiLB we will filter out plays using Scratch.pugh.fielderTrackingErrorsAAA
  data <- data %>% dplyr::filter(levelAbbr != 'mlb' | (fielderLF_startDist >= min.reasonable.of.start.dist & fielderLF_startDist <= max.reasonable.of.start.dist &
                                                         fielderCF_startDist >= min.reasonable.of.start.dist & fielderCF_startDist <= max.reasonable.of.start.dist &
                                                         fielderRF_startDist >= min.reasonable.of.start.dist & fielderRF_startDist <= max.reasonable.of.start.dist)) %>% #&
    # distance >= min.outfielder.responsibility) %>%
    dplyr::filter(levelAbbr != 'mlb' | (fielder1B_startDist < max.reasonable.start.dist &
                                          fielder2B_startDist < max.reasonable.start.dist &
                                          fielderSS_startDist < max.reasonable.start.dist &
                                          fielder3B_startDist < max.reasonable.start.dist)) %>%
    dplyr::filter(levelAbbr != 'mlb' | (direction < 45 & direction > -45 &
                                          fielderLF_startAngle > -45 & fielderLF_startAngle < 45 &
                                          fielderCF_startAngle > -45 & fielderCF_startAngle < 45 &
                                          fielderRF_startAngle > -45 & fielderRF_startAngle < 45 &
                                          fielder1B_startAngle > -45 & fielder1B_startAngle < 45 &
                                          fielder2B_startAngle > -45 & fielder2B_startAngle < 45 &
                                          fielderSS_startAngle > -45 & fielderSS_startAngle < 45 &
                                          fielder3B_startAngle > -45 & fielder3B_startAngle < 45 &
                                          fielderLF_startAngle < fielderCF_startAngle & fielderCF_startAngle < fielderRF_startAngle)) %>%
    dplyr::filter(isBunt == 0) %>%
    mutate(fielder1B_angleDif = bearing - fielder1B_startAngle,
           fielder2B_angleDif = bearing - fielder2B_startAngle,
           fielderSS_angleDif = bearing - fielderSS_startAngle,
           fielder3B_angleDif = bearing - fielder3B_startAngle,
           fielderLF_angleDif = bearing - fielderLF_startAngle,
           fielderCF_angleDif = bearing - fielderCF_startAngle,
           fielderRF_angleDif = bearing - fielderRF_startAngle) %>%
    mutate(outLF = ifelse(isCaught == 1 & fieldedByPos == "LF",1,0),
           outCF = ifelse(isCaught == 1 & fieldedByPos == "CF",1,0),
           outRF = ifelse(isCaught == 1 & fieldedByPos == "RF",1,0),
           out1B = ifelse(isCaught == 1 & fieldedByPos == "1B",1,0),
           out2B = ifelse(isCaught == 1 & fieldedByPos == "2B",1,0),
           outSS = ifelse(isCaught == 1 & fieldedByPos == "SS",1,0),
           out3B = ifelse(isCaught == 1 & fieldedByPos == "3B",1,0)) %>%
    # include distance from landing spot for each fielder
    mutate(ballLandingX = distance*sin(bearing*pi/180),
           ballLandingY = distance*cos(bearing*pi/180)) %>%
    # the lines below reflect the case when the ballLandingPosX is used - 
    # we think bearing/distance combination is the only way to help know 
    # where the ball would have landed, however
    mutate(ballLandingX = ballLandingPosX,
           ballLandingY = ballLandingPosY,
           bearing = atan(ballLandingX / ballLandingY) * 180/pi,
           distance = sqrt(ballLandingX^2 + ballLandingY^2)) %>%
    mutate(fielder1B_startX = fielder1B_startDist * sin(fielder1B_startAngle*pi/180),
           fielder1B_startY = fielder1B_startDist * cos(fielder1B_startAngle*pi/180),
           fielder2B_startX = fielder2B_startDist * sin(fielder2B_startAngle*pi/180),
           fielder2B_startY = fielder2B_startDist * cos(fielder2B_startAngle*pi/180),
           fielderSS_startX = fielderSS_startDist * sin(fielderSS_startAngle*pi/180),
           fielderSS_startY = fielderSS_startDist * cos(fielderSS_startAngle*pi/180),
           fielder3B_startX = fielder3B_startDist * sin(fielder3B_startAngle*pi/180),
           fielder3B_startY = fielder3B_startDist * cos(fielder3B_startAngle*pi/180),
           fielderLF_startX = fielderLF_startDist * sin(fielderLF_startAngle*pi/180),
           fielderLF_startY = fielderLF_startDist * cos(fielderLF_startAngle*pi/180),
           fielderCF_startX = fielderCF_startDist * sin(fielderCF_startAngle*pi/180),
           fielderCF_startY = fielderCF_startDist * cos(fielderCF_startAngle*pi/180),
           fielderRF_startX = fielderRF_startDist * sin(fielderRF_startAngle*pi/180),
           fielderRF_startY = fielderRF_startDist * cos(fielderRF_startAngle*pi/180),
           fielder1B_distFromLanding = sqrt((ballLandingX - fielder1B_startX)^2 + (ballLandingY - fielder1B_startY)^2),
           fielder2B_distFromLanding = sqrt((ballLandingX - fielder2B_startX)^2 + (ballLandingY - fielder2B_startY)^2),
           fielderSS_distFromLanding = sqrt((ballLandingX - fielderSS_startX)^2 + (ballLandingY - fielderSS_startY)^2),
           fielder3B_distFromLanding = sqrt((ballLandingX - fielder3B_startX)^2 + (ballLandingY - fielder3B_startY)^2),
           fielderLF_distFromLanding = sqrt((ballLandingX - fielderLF_startX)^2 + (ballLandingY - fielderLF_startY)^2),
           fielderCF_distFromLanding = sqrt((ballLandingX - fielderCF_startX)^2 + (ballLandingY - fielderCF_startY)^2),
           fielderRF_distFromLanding = sqrt((ballLandingX - fielderRF_startX)^2 + (ballLandingY - fielderRF_startY)^2)) %>%
    # include distance from intercept spot for each fielder
    mutate(fielder1B_endX = fielder1B_endDist * sin(fielder1B_endAngle*pi/180),
           fielder1B_endY = fielder1B_endDist * cos(fielder1B_endAngle*pi/180),
           fielder2B_endX = fielder2B_endDist * sin(fielder2B_endAngle*pi/180),
           fielder2B_endY = fielder2B_endDist * cos(fielder2B_endAngle*pi/180),
           fielderSS_endX = fielderSS_endDist * sin(fielderSS_endAngle*pi/180),
           fielderSS_endY = fielderSS_endDist * cos(fielderSS_endAngle*pi/180),
           fielder3B_endX = fielder3B_endDist * sin(fielder3B_endAngle*pi/180),
           fielder3B_endY = fielder3B_endDist * cos(fielder3B_endAngle*pi/180),
           fielderLF_endX = fielderLF_endDist * sin(fielderLF_endAngle*pi/180),
           fielderLF_endY = fielderLF_endDist * cos(fielderLF_endAngle*pi/180),
           fielderCF_endX = fielderCF_endDist * sin(fielderCF_endAngle*pi/180),
           fielderCF_endY = fielderCF_endDist * cos(fielderCF_endAngle*pi/180),
           fielderRF_endX = fielderRF_endDist * sin(fielderRF_endAngle*pi/180),
           fielderRF_endY = fielderRF_endDist * cos(fielderRF_endAngle*pi/180),
           fielder1B_endDistFromLanding = sqrt((ballLandingX - fielder1B_endX)^2 + (ballLandingY - fielder1B_endY)^2),
           fielder2B_endDistFromLanding = sqrt((ballLandingX - fielder2B_endX)^2 + (ballLandingY - fielder2B_endY)^2),
           fielderSS_endDistFromLanding = sqrt((ballLandingX - fielderSS_endX)^2 + (ballLandingY - fielderSS_endY)^2),
           fielder3B_endDistFromLanding = sqrt((ballLandingX - fielder3B_endX)^2 + (ballLandingY - fielder3B_endY)^2),
           fielderLF_endDistFromLanding = sqrt((ballLandingX - fielderLF_endX)^2 + (ballLandingY - fielderLF_endY)^2),
           fielderCF_endDistFromLanding = sqrt((ballLandingX - fielderCF_endX)^2 + (ballLandingY - fielderCF_endY)^2),
           fielderRF_endDistFromLanding = sqrt((ballLandingX - fielderRF_endX)^2 + (ballLandingY - fielderRF_endY)^2)) %>%
    # include direction fielder needed to go to ball - formula obtained from law of sines
    mutate(angleToBall_1B = 180/pi*asin(distance/fielder1B_distFromLanding*sin((bearing - fielder1B_startAngle)*pi/180)),
           angleToBall_2B = 180/pi*asin(distance/fielder2B_distFromLanding*sin((bearing - fielder2B_startAngle)*pi/180)),
           angleToBall_SS = 180/pi*asin(distance/fielderSS_distFromLanding*sin((bearing - fielderSS_startAngle)*pi/180)),
           angleToBall_3B = 180/pi*asin(distance/fielder3B_distFromLanding*sin((bearing - fielder3B_startAngle)*pi/180)),
           angleToBall_LF = 180/pi*asin(distance/fielderLF_distFromLanding*sin((bearing - fielderLF_startAngle)*pi/180)),
           angleToBall_CF = 180/pi*asin(distance/fielderCF_distFromLanding*sin((bearing - fielderCF_startAngle)*pi/180)),
           angleToBall_RF = 180/pi*asin(distance/fielderRF_distFromLanding*sin((bearing - fielderRF_startAngle)*pi/180))) %>%
    mutate(angleToBall_1B = angleToBall_1B * ifelse(ballLandingX <= (fielder1B_startX/fielder1B_startY)*ballLandingY, -1,1),
           angleToBall_2B = angleToBall_2B * ifelse(ballLandingX <= (fielder2B_startX/fielder2B_startY)*ballLandingY, -1,1),
           angleToBall_SS = angleToBall_SS * ifelse(ballLandingX <= (fielderSS_startX/fielderSS_startY)*ballLandingY, -1,1),
           angleToBall_3B = angleToBall_3B * ifelse(ballLandingX <= (fielder3B_startX/fielder3B_startY)*ballLandingY, -1,1),
           angleToBall_LF = angleToBall_LF * ifelse(ballLandingX <= (fielderLF_startX/fielderLF_startY)*ballLandingY, -1,1),
           angleToBall_CF = angleToBall_CF * ifelse(ballLandingX <= (fielderCF_startX/fielderCF_startY)*ballLandingY, -1,1),
           angleToBall_RF = angleToBall_RF * ifelse(ballLandingX <= (fielderRF_startX/fielderRF_startY)*ballLandingY, -1,1)) %>%
    mutate(angleToBall_1B = ifelse(ballLandingY > (-fielder1B_startX/fielder1B_startY)*ballLandingX + fielder1B_startY + fielder1B_startX^2/fielder1B_startY,180-angleToBall_1B,angleToBall_1B),
           angleToBall_2B = ifelse(ballLandingY > (-fielder2B_startX/fielder2B_startY)*ballLandingX + fielder2B_startY + fielder2B_startX^2/fielder2B_startY,180-angleToBall_2B,angleToBall_2B),
           angleToBall_SS = ifelse(ballLandingY > (-fielderSS_startX/fielderSS_startY)*ballLandingX + fielderSS_startY + fielderSS_startX^2/fielderSS_startY,180-angleToBall_SS,angleToBall_SS),
           angleToBall_3B = ifelse(ballLandingY > (-fielder3B_startX/fielder3B_startY)*ballLandingX + fielder3B_startY + fielder3B_startX^2/fielder3B_startY,180-angleToBall_3B,angleToBall_3B),
           angleToBall_LF = ifelse(ballLandingY > (-fielderLF_startX/fielderLF_startY)*ballLandingX + fielderLF_startY + fielderLF_startX^2/fielderLF_startY,180-angleToBall_LF,angleToBall_LF),
           angleToBall_CF = ifelse(ballLandingY > (-fielderCF_startX/fielderCF_startY)*ballLandingX + fielderCF_startY + fielderCF_startX^2/fielderCF_startY,180-angleToBall_CF,angleToBall_CF),
           angleToBall_RF = ifelse(ballLandingY > (-fielderRF_startX/fielderRF_startY)*ballLandingX + fielderRF_startY + fielderRF_startX^2/fielderRF_startY,180-angleToBall_RF,angleToBall_RF)) %>%
    mutate(toLeft_1B = ifelse(bearing > fielder1B_startAngle,1,0),
           toLeft_2B = ifelse(bearing > fielder2B_startAngle,1,0),
           toLeft_SS = ifelse(bearing > fielderSS_startAngle,1,0),
           toLeft_3B = ifelse(bearing > fielder3B_startAngle,1,0),
           toLeft_LF = ifelse(bearing > fielderLF_startAngle,1,0),
           toLeft_CF = ifelse(bearing > fielderCF_startAngle,1,0),
           toLeft_RF = ifelse(bearing > fielderRF_startAngle,1,0)) %>%
    mutate(toGloveSide_1B = ifelse( (toLeft_1B == 1 & throws1B == "R") | (toLeft_1B == 0 & throws1B == "L"),1,0),
           toGloveSide_2B = ifelse( (toLeft_2B == 1 & throws2B == "R") | (toLeft_2B == 0 & throws2B == "L"),1,0),
           toGloveSide_SS = ifelse( (toLeft_SS == 1 & throwsSS == "R") | (toLeft_SS == 0 & throwsSS == "L"),1,0),
           toGloveSide_3B = ifelse( (toLeft_3B == 1 & throws3B == "R") | (toLeft_3B == 0 & throws3B == "L"),1,0),
           toGloveSide_LF = ifelse( (toLeft_LF == 1 & throwsLF == "R") | (toLeft_LF == 0 & throwsLF == "L"),1,0),
           toGloveSide_CF = ifelse( (toLeft_CF == 1 & throwsCF == "R") | (toLeft_CF == 0 & throwsCF == "L"),1,0),
           toGloveSide_RF = ifelse( (toLeft_RF == 1 & throwsRF == "R") | (toLeft_RF == 0 & throwsRF == "L"),1,0))
  # want to filter out completely uncatchable balls - these appear to be balls that land farther than 165 ft from the fielder
  # mutate(unplayableLF = ifelse(fielderLF_distFromLanding > 165,1,0),
  #        unplayableCF = ifelse(fielderCF_distFromLanding > 165,1,0),
  #        unplayableRF = ifelse(fielderRF_distFromLanding > 165,1,0)) %>%
  # dplyr::filter(!(unplayableLF == 1 & unplayableCF == 1 & unplayableRF == 1))
  
  
  
  # FOR TOR SERIES AT SAHLEN FIELD ONLY:
  milb.park.dimensions <- sqlQuery(lagoon, "SELECT mlbamVenueId, Degree, Distance
                                   from scratch.gahart.lidar_wall_measurements
                                   
                                   union all
                                   
                                   SELECT mlbamVenueId, Degree, Distance
                                   from scratch.gahart.Estimated_Park_Dimensions
                                   --where mlbamVenueId = 2756", stringsAsFactors = FALSE) %>% as_tibble() %>%
    group_by(mlbamVenueId, Degree) %>%
    summarise(Distance = mean(Distance))
  
  data <- data  %>%
    dplyr::filter(bearing < 45 & bearing > -45) %>%
    mutate(bearing_left = floor(bearing), bearing_right = ceiling(bearing)) %>%
    mutate(bearing_right = ifelse(bearing_left == bearing_right, bearing_right + 1, bearing_right)) %>% # in
    # unlikely circumstance that ball is hit directly at a bearing, want to 
    left_join(milb.park.dimensions %>% dplyr::select(mlbamVenueId, Degree, distance_left = Distance),
              by = c("mlbamVenueID" = "mlbamVenueId","bearing_left" = "Degree")) %>%
    left_join(milb.park.dimensions %>% dplyr::select(mlbamVenueId, Degree, distance_right = Distance),
              by = c("mlbamVenueID" = "mlbamVenueId","bearing_right" = "Degree")) %>%
    # in order to get projected fence distance:
    # `1. parameterize the line between the two closest fence points according to x = x_l + (x_r - x_l)*t, 
    #     `y = y_l + (y_r - y_l)*t, where (x_l, y_l) are coordinates of left fence point, (x_r, y_r) are coords of right fence point
    #  2. Find the appropriate t for the ball's trajectory
    #  3. Find the cartesian coordinate of where the ball's trajectory intersects fence
    #  4. Find the distance between where ball intersects fence and where it lands
    mutate(x_l = distance_left * sin(bearing_left*pi/180), y_l = distance_left * cos(bearing_left*pi/180),
           x_r = distance_right * sin(bearing_right*pi/180), y_r = distance_right * cos(bearing_right*pi/180),
           ballTrajectory_T = (bearing - bearing_left) / (bearing_right - bearing_left),
           ballTrajectory_X = x_l + (x_r - x_l)* ballTrajectory_T,
           ballTrajectory_Y = y_l + (y_r-y_l)*ballTrajectory_T,
           battedBallDistanceFromFence_milb = sqrt( (ballTrajectory_X - ballLandingX)^2 + (ballTrajectory_Y - ballLandingY)^2  )) %>%
    # battedBallDistanceFromFence reported by MLB is negative if the ball lands in front of the fence and positive if it lands beyond fence
    # distance formula follows the same convention
    mutate(battedBallDistanceFromFence_milb = battedBallDistanceFromFence_milb * ifelse(distance > distance_left, 1, -1)) %>%
    mutate(battedBallDistanceFromFence = ifelse(!is.na(battedBallDistanceFromFence), battedBallDistanceFromFence,
                                                battedBallDistanceFromFence_milb))
  
  
  # Filtering out all MiLB Plays where the batted ball distance from the fence is > 14. 10 MiLB Plays that match and all but 1 were routine catches. 17 MLB plays that match and all but 3 were routine catches.
  # Only if fielder catches the ball
  data <- data %>% 
    dplyr::mutate(battedBallDistanceFromFence = ifelse((levelAbbr != 'mlb' &  battedBallDistanceFromFence > 3 & isCaught == 1), 3, battedBallDistanceFromFence))
  
  data$gameDate <- as.Date(data$gameDate)
  
  saveRDS(data, "data_for_catch_probability_model.RDS")
  
}

catch.probability.model_2 <- readRDS("//marlins.pvt/FileShares/GroupShares/BaseballOperations_Pvt/Analytics/SShah/OAA/Catch Probability Improvement/outfield_fly_model_xgb_10_24.RDS")

catch.probability.model <- readRDS("outfield_catch_probability_model.pre_var.RDS")



lagoon <- odbcConnect("Lagoon")
outcomes.by.cluster <- sqlQuery(lagoon,
                                "select * from scratch.gahart.resultsByTOPSCluster",
                                stringsAsFactors= FALSE) %>% as_tibble()
close(lagoon)

outcomes.by.cluster <- outcomes.by.cluster %>% 
  mutate(expRV_notCaught = (B1 * single.erv + B2*double.erv + B3*triple.erv) / (B1+B2+B3))
outcomes.by.cluster$expRV_notCaught[is.nan(outcomes.by.cluster$expRV_notCaught)] <- single.erv

outfield.positions <- c("LF","CF","RF")


### add new variable


data <- data %>%
  mutate(spray_diff = if_else(bats == "R", 
                              bearing - direction, 
                              -(bearing - direction)))


# data <-  data %>%
#   mutate(IsOFGoingIn = case_when(
#     fieldedByPos == "LF" & angleToBall_LF <= 72.5 ~ 1,
#     fieldedByPos == "CF" & angleToBall_CF <= 72.5 ~ 1,
#     fieldedByPos == "RF" & angleToBall_RF <= 72.5 ~ 1,
#     TRUE ~ 0
#   ))




# Predicting for MLB outfield data

lf.data <- data %>% dplyr::select(gameDate, mlbamGameID, seqID, ballsBefore, strikesBefore, outsBefore,
                                  throws, bats, batterID, pitcherID, pitchType,
                                  fieldedByPos, levelAbbr, distance, bearing, hangtime, 
                                  exitSpeed, angle, direction, isCaught, fielderLF_startAngle,
                                  fielderLF_startDist, mlbamPlayerLFId, 
                                  ballLandingPosX, ballLandingPosY, 
                                  batterSprintSpeed, battedBallDistanceFromFence, HitSpinRate, HitSpinAngle,
                                  throwsLF, fielderLF_angleDif, 
                                  outLF, outCF, outRF, out1B, out2B, outSS, out3B,
                                  fielderLF_startX, fielderLF_startY, fielderLF_distFromLanding,
                                  angleToBall_LF, toLeft_LF, toGloveSide_LF, TOPSCluster, isError) %>%
  dplyr::filter(out1B == 0 & out2B == 0 & out3B == 0 & outSS == 0, outRF == 0, outCF == 0)
lf.data$expectedCatchProb_LF <- predict(catch.probability.model_2$LF, lf.data, type = "response") 



lf.data <- lf.data %>% 
  mutate(expectedCatchProb_LF= predict(catch.probability.model_2$LF, newdata = lf.data %>% dplyr::select(fielderLF_distFromLanding, hangtime, angleToBall_LF, toGloveSide_LF, battedBallDistanceFromFence) %>% as.matrix()))




lf.data <- lf.data %>% dplyr::select(-out2B, -out1B, -outSS, -out3B)
colnames(lf.data)[colnames(lf.data) == "outLF"] <- "isOut"
colnames(lf.data)[colnames(lf.data) == "mlbamPlayerLFId"] <- "fielderID"
colnames(lf.data)[colnames(lf.data) == "throwsLF"] <- "fielderThrows"
colnames(lf.data) <- sub(pattern = "LF", replacement = "", x = colnames(lf.data))
lf.data$fielderPosition <- "LF"


cf.data <- data %>% dplyr::select(gameDate, mlbamGameID, seqID, ballsBefore, strikesBefore, outsBefore,
                                  throws, bats, batterID, pitcherID, pitchType,
                                  fieldedByPos, levelAbbr, distance, bearing, hangtime, 
                                  exitSpeed, angle, direction, isCaught, fielderCF_startAngle,
                                  fielderCF_startDist, mlbamPlayerCFId, 
                                  ballLandingPosX, ballLandingPosY, 
                                  batterSprintSpeed, battedBallDistanceFromFence, HitSpinRate, HitSpinAngle,
                                  throwsCF, fielderCF_angleDif, 
                                  outLF, outCF, outRF, out1B, out2B, outSS, out3B,
                                  fielderCF_startX, fielderCF_startY, fielderCF_distFromLanding,
                                  angleToBall_CF, toLeft_CF, toGloveSide_CF, TOPSCluster, isError) %>%
  dplyr::filter(out1B == 0 & out2B == 0 & out3B == 0 & outSS == 0,outRF == 0,outLF == 0)



cf.data <- cf.data %>% 
  mutate(expectedCatchProb_CF = predict(catch.probability.model_2$CF, newdata = cf.data %>% dplyr::select(fielderCF_distFromLanding, hangtime, angleToBall_CF, toGloveSide_CF, battedBallDistanceFromFence) %>% as.matrix()))




cf.data$expectedCatchProb_CF <- predict(catch.probability.model_2$CF, cf.data, type = "response")




cf.data <- cf.data %>% dplyr::select( -out2B, -out1B, -outSS, -out3B)
colnames(cf.data)[colnames(cf.data) == "outCF"] <- "isOut"
colnames(cf.data)[colnames(cf.data) == "mlbamPlayerCFId"] <- "fielderID"
colnames(cf.data)[colnames(cf.data) == "throwsCF"] <- "fielderThrows"
colnames(cf.data) <- sub(pattern = "CF", replacement = "", x = colnames(cf.data))
cf.data$fielderPosition <- "CF"


rf.data <- data %>% dplyr::select(gameDate, mlbamGameID, seqID, ballsBefore, strikesBefore, outsBefore,
                                  throws, bats, batterID, pitcherID, pitchType,
                                  fieldedByPos, levelAbbr, distance, bearing, hangtime, 
                                  exitSpeed, angle, direction, isCaught, fielderRF_startAngle,
                                  fielderRF_startDist, mlbamPlayerRFId, 
                                  ballLandingPosX, ballLandingPosY, 
                                  batterSprintSpeed, battedBallDistanceFromFence, HitSpinRate, HitSpinAngle,
                                  throwsRF, fielderRF_angleDif, 
                                  outLF, outCF, outRF, out1B, out2B, outSS, out3B,
                                  fielderRF_startX, fielderRF_startY, fielderRF_distFromLanding,
                                  angleToBall_RF, toLeft_RF, toGloveSide_RF, TOPSCluster, isError) %>% 
  dplyr::filter(out1B == 0 & out2B == 0 & out3B == 0 & outSS == 0,outLF == 0,outCF == 0)


rf.data <- rf.data %>% 
  mutate(expectedCatchProb_RF = predict(catch.probability.model_2$RF, newdata = rf.data %>% dplyr::select(fielderRF_distFromLanding, hangtime, angleToBall_RF, toGloveSide_RF, battedBallDistanceFromFence) %>% as.matrix()))






rf.data$expectedCatchProb_RF <- predict(catch.probability.model_2$RF, rf.data, type = "response")
rf.data <- rf.data %>% dplyr::select(-out2B, -out1B, -outSS, -out3B)
colnames(rf.data)[colnames(rf.data) == "outRF"] <- "isOut"
colnames(rf.data)[colnames(rf.data) == "mlbamPlayerRFId"] <- "fielderID"
colnames(rf.data)[colnames(rf.data) == "throwsRF"] <- "fielderThrows"
colnames(rf.data) <- sub(pattern = "RF", replacement = "", x = colnames(rf.data))
rf.data$fielderPosition <- "RF"

# 4/22/23 - there are significant and known issues of fielder positioning reporting errors
# in AAA in 2023 season. Tim built a scratch table to keep track of those errors. Will filter
# out any play with a significant error.
lagoon <- odbcConnect("Coral")
bad.aaa.start.pos <- sqlQuery(lagoon, paste0("select *
                                              from Scratch.pugh.fielderTrackingErrorsAAA
                                             where gameDate >= '", eval.start.date, "'
                                              and gameDate <= '", eval.end.date, "'"),
                              stringsAsFactors = FALSE) %>%
  as_tibble()
bad.aaa.start.pos$gameDate <- as.Date(bad.aaa.start.pos$gameDate)

bad.aaa.distance <- sqlQuery(lagoon, paste0("select gameDate, mlbamGameID, seqID
                                              from scratch.shah.milbHitDistanceDataErrors
                                             where gameDate >= '", eval.start.date, "'
                                              and gameDate <= '", eval.end.date, "'
                                            and isHitDistanceDataError = 1"),
                             stringsAsFactors = FALSE) %>%
  as_tibble()
bad.aaa.distance$gameDate <- as.Date(bad.aaa.distance$gameDate)


lf.data <- lf.data %>%
  anti_join(bad.aaa.start.pos %>%
              dplyr::filter(fielderLF_startPos_isError == 1),
            by = c("gameDate", "mlbamGameID", "seqID")) %>%
  anti_join(bad.aaa.distance,
            by = c("gameDate", "mlbamGameID", "seqID"))
cf.data <- cf.data %>%
  anti_join(bad.aaa.start.pos %>%
              dplyr::filter(fielderCF_startPos_isError == 1),
            by = c("gameDate", "mlbamGameID", "seqID")) %>%
  anti_join(bad.aaa.distance,
            by = c("gameDate", "mlbamGameID", "seqID"))
rf.data <- rf.data %>%
  anti_join(bad.aaa.start.pos %>%
              dplyr::filter(fielderRF_startPos_isError == 1),
            by = c("gameDate", "mlbamGameID", "seqID")) %>%
  anti_join(bad.aaa.distance,
            by = c("gameDate", "mlbamGameID", "seqID"))

credited <- rbind(lf.data %>% dplyr::filter(outCF == 0 & outRF == 0) %>% dplyr::select(-outCF, -outRF), 
                  cf.data %>% dplyr::filter(outLF == 0 & outRF == 0) %>% dplyr::select(-outLF, -outRF), 
                  rf.data %>% dplyr::filter(outLF == 0 & outCF == 0) %>% dplyr::select(-outLF, -outCF))
colnames(credited) <- gsub(pattern = "_", replacement = "", x = colnames(credited))
credited <- credited %>%
  left_join(outcomes.by.cluster %>% dplyr::select(firstMaxClust, expRV_notCaught),
            by = c("TOPSCluster" = "firstMaxClust")) %>%
  mutate(oaa = isOut - expectedCatchProb) %>%
  mutate(expRV_inPlay = expectedCatchProb * out.erv + (1-expectedCatchProb) * expRV_notCaught) %>%
  mutate(credit = ifelse(isOut == 1, expRV_inPlay - out.erv, expRV_inPlay - expRV_notCaught))

credited <- credited %>%  #include relevant fielder's name in data set
  left_join(players %>% dplyr::select(mlbamID, nameFull), by = c("fielderID" = "mlbamID"))

# Want to find plays for which a certain player's ending spot was within 15 ft of the ball's landing spot.
# On these plays, any other player who was 30+ ft from the ball's landing spot will be excluded from evaluation
all.positions <- c("1B","2B","SS","3B","LF","CF","RF")
data$isWithin15Ft <- apply(data[, paste0("fielder", all.positions, "_endDistFromLanding")], 1, 
                           function(x) {sum(ifelse(x <= distance.to.be.under.ball, 1, 0))})
data$isWithin15Ft <- ifelse(data$isWithin15Ft > 1, 1, data$isWithin15Ft)


# Also want to find out, for each play, what the "team" catch probability was. We'll say that whoever
# has the highest catch probability of the three outfielders is the one that is being judged (so if CF has 95%
# and RF has 50%, there is a team catch probability of 95%)
team.credited <- full_join(lf.data %>% dplyr::filter(distance >= min.outfielder.responsibility) %>%
                             dplyr::select(gameDate, mlbamGameID, seqID, TOPSCluster, mlbamId_LF = fielderID,
                                           LFCatchProb = expectedCatchProb_, isOutLF = isOut,
                                           fielderLF_startDist = fielder_startDist, fielderLF_startAngle = fielder_startAngle),
                           cf.data %>%  dplyr::filter(distance >= min.outfielder.responsibility) %>%
                             dplyr::select(gameDate, mlbamGameID, seqID, TOPSCluster, mlbamId_CF = fielderID,
                                           CFCatchProb = expectedCatchProb_, isOutCF = isOut,
                                           fielderCF_startDist = fielder_startDist, fielderCF_startAngle = fielder_startAngle),
                           by= c("gameDate","mlbamGameID","seqID", "TOPSCluster")) %>%
  full_join(., rf.data %>%  dplyr::filter(distance >= min.outfielder.responsibility) %>%
              dplyr::select(gameDate, mlbamGameID, seqID, TOPSCluster, mlbamID_RF = fielderID,
                            RFCatchProb = expectedCatchProb_, isOutRF = isOut,
                            fielderRF_startDist = fielder_startDist, fielderRF_startAngle = fielder_startAngle),
            by= c("gameDate","mlbamGameID","seqID","TOPSCluster"))
team.credited <- team.credited %>% mutate(isOut = ifelse(isOutLF + isOutCF + isOutRF >= 1,1,0))
team.credited$maxCatchProb <- apply(team.credited %>% dplyr::select(LFCatchProb, CFCatchProb, RFCatchProb), 1, max)
team.credited <- team.credited %>% mutate(maxCatchPosition = 
                                            ifelse(LFCatchProb == maxCatchProb, "LF", ifelse(CFCatchProb == maxCatchProb, "CF", "RF")))
team.credited <- team.credited %>% left_join(outcomes.by.cluster %>% dplyr::select(firstMaxClust, expRV_notCaught),
                                             by = c("TOPSCluster" = "firstMaxClust")) %>%
  mutate(oaa = isOut - maxCatchProb) %>%
  mutate(expRV_inPlay = maxCatchProb * out.erv + (1-maxCatchProb) * expRV_notCaught) %>%
  mutate(credit = ifelse(isOut == 1, expRV_inPlay - out.erv, expRV_inPlay - expRV_notCaught))

# Also want to find out how much credit/debit each outfielder should get when compared to all
# other outfielders (not just their position)
lf.plays <- data %>% dplyr::filter(outCF == 0 & outRF == 0 & out1B == 0 & out2B == 0 & outSS == 0 & out3B == 0) %>%
  dplyr::filter(distance > min.outfielder.responsibility) %>%
  dplyr::select(mlbamGameID, gameDate, seqID, fielderID = mlbamPlayerLFId, isOut = outLF, distFromLanding = fielderLF_distFromLanding, hangtime, angleToBall = angleToBall_LF,
                toGloveSide = toGloveSide_LF, battedBallDistanceFromFence, TOPSCluster,spray_diff) %>%
  mutate(Pos = "LF")
cf.plays <- data %>% dplyr::filter(outLF == 0 & outRF == 0 & out1B == 0 & out2B == 0 & outSS == 0 & out3B == 0) %>%
  dplyr::filter(distance > min.outfielder.responsibility) %>%
  dplyr::select(mlbamGameID, gameDate, seqID, fielderID = mlbamPlayerCFId, isOut = outCF, distFromLanding = fielderCF_distFromLanding, hangtime, angleToBall = angleToBall_CF,
                toGloveSide = toGloveSide_CF, battedBallDistanceFromFence, TOPSCluster,spray_diff) %>%
  mutate(Pos = "CF")
rf.plays <- data %>% dplyr::filter(outLF == 0 & outCF == 0 & out1B == 0 & out2B == 0 & outSS == 0 & out3B == 0) %>% 
  dplyr::filter(distance > min.outfielder.responsibility) %>%
  dplyr::select(mlbamGameID, gameDate, seqID, fielderID = mlbamPlayerRFId, isOut = outRF, distFromLanding = fielderRF_distFromLanding, hangtime, angleToBall = angleToBall_RF,
                toGloveSide = toGloveSide_RF, battedBallDistanceFromFence, TOPSCluster,spray_diff) %>%
  mutate(Pos = "RF")
data.all.of <- rbind(lf.plays, cf.plays, rf.plays)
data.all.of$expectedCatchProb <- predict(catch.probability.model$ALL, data.all.of, type = "response")
data.all.of <- data.all.of %>% left_join(outcomes.by.cluster %>% dplyr::select(firstMaxClust, expRV_notCaught),
                                         by = c("TOPSCluster" = "firstMaxClust")) %>%
  mutate(oaa = isOut - expectedCatchProb) %>%
  mutate(expRV_inPlay = expectedCatchProb * out.erv + (1-expectedCatchProb) * expRV_notCaught) %>%
  mutate(credit = ifelse(isOut == 1, expRV_inPlay - out.erv, expRV_inPlay - expRV_notCaught))
data.all.of <- data.all.of %>% left_join(players %>% dplyr::select(mlbamID, nameFull),
                                         by = c("fielderID" = "mlbamID"))

####
# Filter out certain balls that are impossible for one fielder to catch
# 1. If ball is to the left of LF or right of RF, it is impossible for CF to catch
# In addition, if ball is not within 30 ft of fielder but is within 15 ft of someone else, not possible to catch
# In addition, filter out balls for which an error was committed on the play by another fielder
cf.cant.catch <- team.credited %>% inner_join(data %>% dplyr::select(gameDate, mlbamGameID, seqID, bearing, distance,spray_diff), 
                                              by = c("gameDate", "mlbamGameID","seqID")) %>%
  dplyr::filter(bearing < fielderLF_startAngle | bearing > fielderRF_startAngle) %>%
  dplyr::select(gameDate, mlbamGameID, seqID) %>%
  rbind(data %>% 
          dplyr::filter(isWithin15Ft == 1 & fielderCF_endDistFromLanding >= distance.to.not.be.responsible) %>%
          dplyr::select(gameDate, mlbamGameID, seqID)) %>%
  rbind(data %>% 
          dplyr::filter(isError == 1 & (is.na(errorPosition) | errorPosition != "CF")) %>%
          dplyr::select(gameDate, mlbamGameID, seqID))  %>%
  rbind(data %>%
          dplyr::filter(fieldedByPos == "P") %>%
          dplyr::select(gameDate, mlbamGameID, seqID)) %>%
  rbind(data %>%
          dplyr::filter(distance < min.outfielder.responsibility) %>%
          dplyr::select(gameDate, mlbamGameID, seqID))


# 2. If ball is to the left of center, it is impossible for RF to catch; if to right of center, it is impossible for LF to catch
lf.cant.catch <- team.credited %>% 
  inner_join(data %>% dplyr::select(gameDate, mlbamGameID, seqID, bearing, distance,spray_diff), 
             by = c("gameDate", "mlbamGameID","seqID")) %>%
  dplyr::filter(bearing > 0) %>%
  dplyr::select(gameDate, mlbamGameID, seqID) %>%
  rbind(data %>% 
          dplyr::filter(isWithin15Ft == 1 & fielderLF_endDistFromLanding >= distance.to.not.be.responsible) %>%
          dplyr::select(gameDate, mlbamGameID, seqID)) %>%
  rbind(data %>% 
          dplyr::filter(isError == 1 & (is.na(errorPosition) | errorPosition != "LF")) %>%
          dplyr::select(gameDate, mlbamGameID, seqID)) %>%
  rbind(data %>%
          dplyr::filter(fieldedByPos == "P") %>%
          dplyr::select(gameDate, mlbamGameID, seqID))   %>%
  rbind(data %>%
          dplyr::filter(distance < min.outfielder.responsibility) %>%
          dplyr::select(gameDate, mlbamGameID, seqID))


rf.cant.catch <- team.credited %>%
  inner_join(data %>% dplyr::select(gameDate, mlbamGameID, seqID, bearing, distance,spray_diff), 
             by = c("gameDate", "mlbamGameID","seqID")) %>%
  dplyr::filter(bearing < 0) %>%
  dplyr::select(gameDate, mlbamGameID, seqID) %>%
  rbind(data %>% 
          dplyr::filter(isWithin15Ft == 1 & fielderRF_endDistFromLanding >= distance.to.not.be.responsible) %>%
          dplyr::select(gameDate, mlbamGameID, seqID)) %>%
  rbind(data %>% 
          dplyr::filter(isError == 1 & (is.na(errorPosition) | errorPosition != "RF")) %>%
          dplyr::select(gameDate, mlbamGameID, seqID))  %>%
  rbind(data %>%
          dplyr::filter(fieldedByPos == "P") %>%
          dplyr::select(gameDate, mlbamGameID, seqID)) %>%
  rbind(data %>%
          dplyr::filter(distance < min.outfielder.responsibility) %>%
          dplyr::select(gameDate, mlbamGameID, seqID))




# 3. Find some distance for which if the ball is that much closer to corner than CF, it is impossible for CF to catch
cf.corner.comparison <- team.credited %>%
  inner_join(data %>% dplyr::select(gameDate, mlbamGameID, seqID, bearing, distance,
                                    fielderLF_distFromLanding, fielderCF_distFromLanding, fielderRF_distFromLanding,spray_diff), 
             by = c("gameDate", "mlbamGameID","seqID")) %>%
  mutate(cf.farther.than.corner = fielderCF_distFromLanding - ifelse(fielderLF_distFromLanding < fielderRF_distFromLanding,
                                                                     fielderLF_distFromLanding, fielderRF_distFromLanding),
         isOutCorner = ifelse(isOutLF == 1 | isOutRF == 1, 1, 0)) %>%
  dplyr::select(gameDate, mlbamGameID, seqID, cf.farther.than.corner, contains("distFromLanding"), isOutCF, isOutCorner) 
# cf.corner.comparison %>%  group_by(cf.farther.than.corner = round(cf.farther.than.corner/2.5)*2.5) %>%
#   summarise(cf.outs = sum(isOutCF), corner.outs = sum(isOutCorner)) %>% mutate(pct.cf.out = cf.outs/(cf.outs+corner.outs)) %>%
#   dplyr::filter(pct.cf.out < .01)
# From this, we see that there have been 0 balls caught by the CF when the CF was more than 72.5 ft farther from the ball
# than the nearest corner outfielder. So, any plays for which the CF is greater than this distance away will be excluded
cf.cant.catch <- rbind(cf.cant.catch,
                       cf.corner.comparison %>% dplyr::filter(cf.farther.than.corner >= 72.5) %>%
                         dplyr::select(gameDate, mlbamGameID, seqID))

# Additionally: Look at the deflected balls from the finding_deflected_balls.R file, which finds
# the subset of balls which were touched in the air or on the short hop by a fielder. If one fielder
# deflects the ball, then all other fielders cannot be evaluated on those balls
# lagoon <- odbcConnect("Coral")
# # To deflected balls function, pass in a tibble containing all plays
# # where multiple fielders had at least 1% chance of making the catch. Could theoretically
# # pass in all plays, though that would likely take more time than intended
# plays.with.multiple.fielders <- team.credited %>%
#   dplyr::filter(!isOutLF & !isOutCF & !isOutRF) %>% # if caught, clearly no deflection
#   left_join(b1.data %>%
#               dplyr::filter(isOut == 0) %>%
#               dplyr::select(gameDate, mlbamGameID, seqID, catchProb1B = expectedCatchProb_),
#             by = c("gameDate", "mlbamGameID", "seqID")) %>%
#   left_join(b2.data %>% 
#               dplyr::filter(isOut == 0) %>%
#               dplyr::select(gameDate, mlbamGameID, seqID, catchProb2B = expectedCatchProb_),
#             by = c("gameDate", "mlbamGameID", "seqID")) %>%
#   left_join(ss.data %>% 
#               dplyr::filter(isOut == 0) %>%
#               dplyr::select(gameDate, mlbamGameID, seqID, catchProbSS = expectedCatchProb_),
#             by = c("gameDate", "mlbamGameID", "seqID")) %>%
#   left_join(b3.data %>% 
#               dplyr::filter(isOut == 0) %>%
#               dplyr::select(gameDate, mlbamGameID, seqID, catchProb3B = expectedCatchProb_),
#             by = c("gameDate", "mlbamGameID", "seqID")) %>%
#   mutate(isLFOpp = ifelse(!is.na(LFCatchProb) & LFCatchProb > .01, 1, 0)) %>%
#   mutate(isCFOpp = ifelse(!is.na(CFCatchProb) & CFCatchProb > .01, 1, 0)) %>%
#   mutate(isRFOpp = ifelse(!is.na(RFCatchProb) & RFCatchProb > .01, 1, 0)) %>%
#   mutate(is1BOpp = ifelse(!is.na(catchProb1B) & catchProb1B > .01, 1, 0)) %>%
#   mutate(is2BOpp = ifelse(!is.na(catchProb2B) & catchProb2B > .01, 1, 0)) %>%
#   mutate(isSSOpp = ifelse(!is.na(catchProbSS) & catchProbSS > .01, 1, 0)) %>%
#   mutate(is3BOpp = ifelse(!is.na(catchProb3B) & catchProb3B > .01, 1, 0)) %>%
#   mutate(numPossibleFielders = isLFOpp + isCFOpp + isRFOpp + is1BOpp + is2BOpp + isSSOpp + is3BOpp) %>%
#   dplyr::filter(numPossibleFielders >= 2) %>%
#   dplyr::select(gameDate, mlbamGameID, seqID, ct = numPossibleFielders)


# if (look.for.deflections) {
#   system.time(
#     point.and.time.results <- find_deflected_balls(plays = plays.with.multiple.fielders)
#   )
# }
#deflected.balls <- point.and.time.results$deflections
deflected.balls <- sqlQuery(lagoon, paste0("select * from scratch.gahart.deflectedFlyballs
                            where gameDate >= '", eval.start.date, "' and gameDate <= '", eval.end.date, "'
                            UNION ALL               
                            select * from scratch.shah.deflectedFlyballs
                            where gameDate >= '", eval.start.date, "' and gameDate <= '", eval.end.date, "'"),
                            stringsAsFactors = FALSE) %>% as_tibble()
#player.velos <- point.and.time.results$player_velos


# Don't want balls that aren't deflected by a fielder. Seems like some instances
# of positionId of 1, 10, or 14, seemingly all with balls off the wall where that
# ball off the wall was not properly recorded
deflected.balls <- deflected.balls %>%
  dplyr::filter(positionId %in% 1:9) %>%
  dplyr::filter(flag == 1) # from below...flag seems to have value

# deflected.balls <- sqlQuery(lagoon, "SELECT * from scratch.gahart.deflectedFlyballs 
#                                     WHERE flag = 1",
#                             stringsAsFactors = FALSE) %>% as_tibble() # flag indicates whether or not
# deflection occured before ball landed, was fielded, or hit wall
close(lagoon)

if (nrow(deflected.balls) > 0) {
  lf.cant.catch <- rbind(lf.cant.catch,
                         deflected.balls %>% dplyr::filter(positionId != 7) %>%
                           dplyr::select(gameDate, mlbamGameID, seqID))
  cf.cant.catch <- rbind(cf.cant.catch,
                         deflected.balls %>% dplyr::filter(positionId != 8) %>%
                           dplyr::select(gameDate, mlbamGameID, seqID))
  rf.cant.catch <- rbind(rf.cant.catch,
                         deflected.balls %>% dplyr::filter(positionId != 9) %>%
                           dplyr::select(gameDate, mlbamGameID, seqID))
}

# CF Only balls: If CF has catch prob that is within 10% of than any other fielder and neither of the other
# fielders caught or deflected the ball, then it is the CF's ball only
#     But, if RF or LF had a catch probability of 99% or higher, then rules slightly adjusted
cf.only <- team.credited %>% 
  dplyr::filter(LFCatchProb < .99 & RFCatchProb < .99 &
                  CFCatchProb > LFCatchProb - .10 & CFCatchProb > RFCatchProb - .10)

# Similarly: any ball where the RF has a 99% catch prob or above and CF has 99% catch prob or below
# is not for CF.
cf.cant.catch <- rbind(cf.cant.catch, 
                       team.credited %>%
                         dplyr::filter((LFCatchProb > .99 & CFCatchProb < .99) |
                                         (RFCatchProb > .99 & CFCatchProb < .99)) %>%
                         dplyr::select(gameDate, mlbamGameID, seqID)) 



if (nrow(deflected.balls) > 0) {
  cf.only <- cf.only %>%
    left_join(deflected.balls %>%
                mutate(gameDate = as.Date(gameDate)) %>%
                dplyr::select(gameDate, mlbamGameID, seqID, positionId), 
              by = c("gameDate", "mlbamGameID", "seqID")) %>%
    mutate(isDeflectedByCorner = ifelse(is.na(positionId), 0,
                                        ifelse(positionId == 7 | positionId == 9, 1, 0))) %>%
    dplyr::filter(isDeflectedByCorner == 0 & isOutLF == 0 & isOutRF == 0) %>%
    dplyr::select(-positionId, -isDeflectedByCorner)
}
# On plays where the CF has a catch prob of 95% or higher and the corner outfielder
# has a catch prob of 15% less than the CF, then the corner outfielder can't be evaluated
# for the ball
cf.only <- rbind(cf.only,
                 team.credited %>%
                   dplyr::filter(CFCatchProb >= .95 &
                                   CFCatchProb - LFCatchProb >= .15 & CFCatchProb - RFCatchProb >= .15 ))

## Update 6/18/24: A ball can't be assigned to Cf only if it has already been assigned to CF cant catch
cf.only <- cf.only %>% 
  anti_join(cf.cant.catch,
            by = c("gameDate", "mlbamGameID", "seqID"))

lf.cant.catch <- rbind(lf.cant.catch, cf.only %>% dplyr::select(gameDate, mlbamGameID, seqID))
rf.cant.catch <- rbind(rf.cant.catch, cf.only %>% dplyr::select(gameDate, mlbamGameID, seqID))

# In lieu of all of the rules above: If a player is charged with an error on a play by the stringer,
# then that player can still be evaluated for that play
lf.cant.catch <- lf.cant.catch %>% 
  anti_join(data %>%
              dplyr::filter(!is.na(errorPosition) & errorPosition == "LF"),
            by = c("gameDate", "mlbamGameID", "seqID"))
cf.cant.catch <- cf.cant.catch %>% 
  anti_join(data %>%
              dplyr::filter(!is.na(errorPosition) & errorPosition == "CF"),
            by = c("gameDate", "mlbamGameID", "seqID"))
rf.cant.catch <- rf.cant.catch %>% 
  anti_join(data %>%
              dplyr::filter(!is.na(errorPosition) & errorPosition == "RF"),
            by = c("gameDate", "mlbamGameID", "seqID"))




# Now, remove all rows from the various tables corresponding to these plays
lf.plays <- lf.plays %>% anti_join(lf.cant.catch, by = c("gameDate", "mlbamGameID", "seqID"))
cf.plays <- cf.plays %>% anti_join(cf.cant.catch, by = c("gameDate", "mlbamGameID", "seqID"))
rf.plays <- rf.plays %>% anti_join(rf.cant.catch, by = c("gameDate", "mlbamGameID", "seqID"))

data.all.of <- data.all.of %>% anti_join(rbind(lf.cant.catch %>% mutate(Pos = "LF"),
                                               cf.cant.catch %>% mutate(Pos = "CF"),
                                               rf.cant.catch %>% mutate(Pos = "RF")),
                                         by = c("gameDate", "mlbamGameID", "seqID", "Pos"))

credited <- credited %>% anti_join(rbind(lf.cant.catch %>% mutate(fielderPosition = "LF"),
                                         cf.cant.catch %>% mutate(fielderPosition = "CF"),
                                         rf.cant.catch %>% mutate(fielderPosition = "RF")),
                                   by = c("gameDate", "mlbamGameID", "seqID", "fielderPosition"))




# Regress routine flyballs - research in routinePlaysRegression.R found that the most 
# predictive prediction for future seasons was to define a routine play as 
# any with a catch prob over 92%, and to regress those by half
credited <- credited %>%
  mutate(oaa = ifelse(expectedCatchProb >= .92, oaa * .5, oaa),
         credit = ifelse(expectedCatchProb >= .92, credit * .5, credit))
data.all.of <- data.all.of %>%
  mutate(oaa = ifelse(expectedCatchProb >= .92, oaa * .5, oaa),
         credit = ifelse(expectedCatchProb >= .92, credit * .5, credit))



### Write to db
if (writeToDB) {
  
  
  db2<-dbConnect(odbc::odbc(),
                 .connection_string = "Driver={SQL Server};Server=MM22-SQL01-BB;Database=Scratch;Trusted_Connection=yes;")
  lagoon <- odbcConnect("Coral")
  
  # 2 extra columns Myles wants to see in the outfield tables for individual players:
  # Bits representing if halved or debited credited (i.e. if catch prob over 92%)
  credited <- credited %>%
    mutate(isHalfCredit = ifelse(isOut == 1 & expectedCatchProb >= .92, 1, 0),
           isHalfDebit = ifelse(isOut == 0 & expectedCatchProb >= .92, 1, 0))
  
  data.all.of <- data.all.of %>%
    mutate(isHalfCredit = ifelse(isOut == 1 & expectedCatchProb >= .92, 1, 0),
           isHalfDebit = ifelse(isOut == 0 & expectedCatchProb >= .92, 1, 0))
  
  
  ofTable <- Id(catalog= "Scratch", schema = "shah", table = "creditedOutfieldPlays_xgb_10_24")
  dbWriteTable(db2,ofTable,credited,overwrite = TRUE)
  
  ofTableAll <- Id(catalog= "Scratch", schema = "shah", table = "creditedOutfieldPlays_AllPositions_updated_one_var_last")
  dbWriteTable(db2,ofTableAll, data.all.of, overwrite=TRUE)
}






