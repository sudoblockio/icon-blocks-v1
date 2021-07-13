//+build integration

package crud_test

import (
	"github.com/geometry-labs/icon-blocks/fixtures"
	"github.com/geometry-labs/icon-blocks/models"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("BlockModel", func() {
	testFixtures, _ := fixtures.LoadTestFixtures(fixtures.Block_raws_fixture)

	Describe("blockModel with postgres", func() {
		// TODO: RM delete - does not ever happen

		Context("insert in block table", func() {
			for _, fixture := range testFixtures {
				block := fixture.GetBlock(fixture.Input)
				BeforeEach(func() {
					blockModel.Delete("Signature = ?", block.Signature)
				})
				It("predefined block insert", func() {
					blockModel.RetryCreate(block)
					found, _ := blockModel.FindOne("Signature = ?", block.Signature)
					Expect(found.Hash).To(Equal(block.Hash))
				}) // It
			} // For
		}) // context

		// TODO: RM delete - does not ever happen
		Context("update in block table", func() {
			for _, fixture := range testFixtures {
				block := fixture.GetBlock(fixture.Input)
				BeforeEach(func() {
					blockModel.Delete("Signature = ?", block.Signature)
					blockModel.RetryCreate(block)
				})
				It("predefined block update", func() {
					blockModel.Update(block, &models.Block{Type: "blockRaw"}, "Signature = ?", block.Signature)
					found, _ := blockModel.FindOne("Signature = ?", block.Signature)
					Expect(found.Type).To(Equal("blockRaw"))
				}) // It
			} // For
		}) // context

		Context("delete in block table", func() {
			for _, fixture := range testFixtures {
				block := fixture.GetBlock(fixture.Input)
				BeforeEach(func() {
					blockModel.Delete("Signature = ?", block.Signature)
					blockModel.RetryCreate(block)
				})
				It("predefined block delete", func() {
					blockModel.Delete("Signature = ?", block.Signature)
					found, _ := blockModel.FindOne("Signature = ?", block.Signature)
					Expect(found.Hash).To(Equal(""))
				}) // It
			} // For
		}) // context

	}) // Describe
}) // Describe
