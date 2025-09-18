package carousal

import "testing"

func TestCarousal_SuccessfulElection(t *testing.T) {
	const nodeCount = 4
	
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	nodes := make([]*MockNode, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodes[i] = NewMockNode(ctrl)
		nodes[i].EXPECT().GetCurrentView().Return(int64(1)).AnyTimes()
		nodes[i].EXPECT().GetCurrentBlock().Return(&Block{}).AnyTimes()
	}