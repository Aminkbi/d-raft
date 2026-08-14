package raft

import "slices"

// Membership is a stable or joint-consensus voting configuration. Voters is
// the incoming (or stable) voter set. VotersOutgoing is non-empty only during
// joint consensus. LearnersNext records the learner set that takes effect when
// a joint configuration is finalized.
type Membership struct {
	Voters         []NodeID
	VotersOutgoing []NodeID
	Learners       []NodeID
	LearnersNext   []NodeID
}

// Joint reports whether both old and new voter majorities are required.
func (m Membership) Joint() bool { return len(m.VotersOutgoing) > 0 }

// IsVoter reports whether id belongs to either voter set.
func (m Membership) IsVoter(id NodeID) bool { return m.isVoter(id) }

// HasQuorum reports whether nodes contain a majority of each active voter set.
func (m Membership) HasQuorum(nodes []NodeID) bool {
	return m.quorum(func(id NodeID) bool { return slices.Contains(nodes, id) })
}

// StableMembership constructs a canonical stable configuration.
func StableMembership(voters, learners []NodeID) Membership {
	return stableMembership(voters, learners)
}

// ValidateMembership reports whether m is canonical and contained in universe.
func ValidateMembership(m Membership, universe []NodeID) bool {
	canonicalUniverse := slices.Clone(universe)
	slices.Sort(canonicalUniverse)
	return validateMembership(m, canonicalUniverse)
}

// CloneMembership returns a deep copy.
func CloneMembership(m Membership) Membership {
	m.Voters = slices.Clone(m.Voters)
	m.VotersOutgoing = slices.Clone(m.VotersOutgoing)
	m.Learners = slices.Clone(m.Learners)
	m.LearnersNext = slices.Clone(m.LearnersNext)
	return m
}

func stableMembership(voters, learners []NodeID) Membership {
	result := Membership{Voters: slices.Clone(voters), Learners: slices.Clone(learners)}
	slices.Sort(result.Voters)
	slices.Sort(result.Learners)
	return result
}

func initialMembership(config Config, universe []NodeID) (Membership, bool) {
	if len(config.Voters) == 0 && len(config.Learners) == 0 {
		return stableMembership(universe, nil), true
	}
	membership := stableMembership(config.Voters, config.Learners)
	return membership, validateMembership(membership, universe)
}

func transitionMembership(current Membership, entry Entry, universe []NodeID) (Membership, bool) {
	switch entry.Type {
	case EntryConfigJoint:
		if len(entry.Data) != 0 || current.Joint() || !entry.Membership.Joint() || !validateMembership(entry.Membership, universe) || !slices.Equal(entry.Membership.VotersOutgoing, current.Voters) {
			return Membership{}, false
		}
		expectedLearners := make([]NodeID, 0, len(entry.Membership.LearnersNext))
		for _, learner := range entry.Membership.LearnersNext {
			if !slices.Contains(entry.Membership.VotersOutgoing, learner) {
				expectedLearners = append(expectedLearners, learner)
			}
		}
		if !slices.Equal(entry.Membership.Learners, expectedLearners) {
			return Membership{}, false
		}
		return CloneMembership(entry.Membership), true
	case EntryConfigFinal:
		if len(entry.Data) != 0 || !current.Joint() || entry.Membership.Joint() || !validateMembership(entry.Membership, universe) || !slices.Equal(entry.Membership.Voters, current.Voters) || !slices.Equal(entry.Membership.Learners, current.LearnersNext) {
			return Membership{}, false
		}
		return CloneMembership(entry.Membership), true
	default:
		if !membershipIsZero(entry.Membership) {
			return Membership{}, false
		}
		return current, true
	}
}

func membershipIsZero(m Membership) bool {
	return len(m.Voters) == 0 && len(m.VotersOutgoing) == 0 && len(m.Learners) == 0 && len(m.LearnersNext) == 0
}

func (m Membership) isVoter(id NodeID) bool {
	return slices.Contains(m.Voters, id) || slices.Contains(m.VotersOutgoing, id)
}

func (m Membership) quorum(matches func(NodeID) bool) bool {
	if !majorityMatches(m.Voters, matches) {
		return false
	}
	return len(m.VotersOutgoing) == 0 || majorityMatches(m.VotersOutgoing, matches)
}

func majorityMatches(voters []NodeID, matches func(NodeID) bool) bool {
	count := 0
	for _, voter := range voters {
		if matches(voter) {
			count++
		}
	}
	return count >= len(voters)/2+1
}

func voterUnion(m Membership) []NodeID {
	result := slices.Clone(m.Voters)
	for _, voter := range m.VotersOutgoing {
		if !slices.Contains(result, voter) {
			result = append(result, voter)
		}
	}
	slices.Sort(result)
	return result
}

func validateMembership(m Membership, universe []NodeID) bool {
	if len(m.Voters) == 0 || !canonicalNodeSet(m.Voters, universe) || !canonicalNodeSet(m.VotersOutgoing, universe) || !canonicalNodeSet(m.Learners, universe) || !canonicalNodeSet(m.LearnersNext, universe) {
		return false
	}
	if !m.Joint() && len(m.LearnersNext) != 0 {
		return false
	}
	for _, voter := range m.Voters {
		if slices.Contains(m.Learners, voter) || slices.Contains(m.LearnersNext, voter) {
			return false
		}
	}
	for _, voter := range m.VotersOutgoing {
		if slices.Contains(m.Learners, voter) {
			return false
		}
	}
	return true
}

func canonicalNodeSet(nodes, universe []NodeID) bool {
	if !slices.IsSorted(nodes) {
		return false
	}
	for index, node := range nodes {
		if node == "" || !slices.Contains(universe, node) || index > 0 && node == nodes[index-1] {
			return false
		}
	}
	return true
}

func membershipsEqual(left, right Membership) bool {
	return slices.Equal(left.Voters, right.Voters) && slices.Equal(left.VotersOutgoing, right.VotersOutgoing) && slices.Equal(left.Learners, right.Learners) && slices.Equal(left.LearnersNext, right.LearnersNext)
}

// MembershipsEqual compares every role set.
func MembershipsEqual(left, right Membership) bool { return membershipsEqual(left, right) }
