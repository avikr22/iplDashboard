package io.javabrains.ipldashboard.controller;

import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import io.javabrains.ipldashboard.model.Team;
import io.javabrains.ipldashboard.repository.MatchRepository;
import io.javabrains.ipldashboard.repository.TeamRepository;

@CrossOrigin
@RestController
public class TeamController {
	
	private TeamRepository teamRepository;
	private MatchRepository  matchRepository;

	public TeamController(TeamRepository teamRepository, MatchRepository matchRepository) {
		this.teamRepository = teamRepository;
		this.matchRepository = matchRepository;
	}



	@GetMapping("/team/{teamName}")
	public Team getTeam(@PathVariable String teamName) {
		Team team =  this.teamRepository.findByTeamName(teamName);
		//This tells to fetch the page with 0 index and 4 contents from there. But its not a good idea to
		//wire this in Controller. So we will comment this and move this logic to Repository
		//Pageable pageable = PageRequest.of(0, 4);
		team.setMatches(matchRepository.findLatestMatchesByTeam(teamName, 4));
		return team;
	}
}
