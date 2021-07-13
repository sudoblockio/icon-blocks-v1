//+build integration

package crud_test

import (
	"github.com/geometry-labs/icon-blocks/crud"
	"github.com/geometry-labs/icon-blocks/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gorm.io/gorm"
)

var _ = Describe("BlockModel", func() {
	testFixtures, _ := fixtures.LoadTestFixtures(fixtures.Block_raws_fixture)

	Describe("blockModel with postgres", func() {

		Context("insert and find in block table", func() {
			for _, fixture := range testFixtures {
				block := fixture.GetBlock(fixture.Input)
				BeforeEach(func() {
					Delete(blockModel, "Signature = ?", block.Signature)
				})
				It("predefined block insert", func() {
					_, err := blockModel.RetryCreate(block)
					if err != nil {
						Expect(err).To(nil)
					}
					found, _ := blockModel.FindOne("Signature = ?", block.Signature)
					Expect(found.Hash).To(Equal(block.Hash))
				}) // It
			} // For
		}) // context

	}) // Describe
}) // Describe

//func Update(m *crud.BlockModel, oldBlock *models.Block, newBlock *models.Block, whereClause ...interface{}) *gorm.DB {
//	tx := m.GetDB().Model(oldBlock).Where(whereClause[0], whereClause[1:]).Updates(newBlock)
//	return tx
//}

func Delete(m *crud.BlockModel, conds ...interface{}) *gorm.DB {
	tx := m.GetDB().Delete(m.GetModel(), conds...)
	return tx
}
