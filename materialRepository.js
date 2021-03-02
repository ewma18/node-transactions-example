const config = require('../../config.js');
const dbClient = require('pd-node-mysql-client').getClient(config.getConnections().mysql);

const materialRepository = {
	createMaterialSubject: async function (Id, SubjectId, transaction) {

		const query =
			`INSERT INTO dbo.MaterialSubject(MaterialId, SubjectId)
							VALUES (:Id,:SubjectId)`;


		var connection = await dbClient.getConnectionAsync(transaction);
		var result = await connection.queryAsync(query, {
			Id,
			SubjectId
		});
		if (result && result.affetedRows == 0) {}
	},
	createMaterial: async function (material, transaction) {
		const query =
			`INSERT INTO dbo.Material(Id, MaterialCategoryId, MaterialTypeId, MaterialFormatId, ThumbnailUrl, UserId, OwnerId, Name, Description, LanguageId, CountryId, CourseId, UniversityId, SchoolId, AreaId, StudentTypeId, IsPremium, StudentDegreeLevelId, Period)
								VALUES (:Id, :CategoryTypeId, :MaterialTypeId, :MaterialFormatId, :Thumbnail, :UserId, :OwnerId, :Title, :Description, :LanguageId, :CountryId, :CourseId, :UniversityId, :SchoolId, :AreaId, :StudentTypeId, :IsPremium,:StudentDegreeLevelId, :Period);`


		var connection = await dbClient.getConnectionAsync(transaction);
		var result = await connection.queryAsync(query, material);
		return result[0];
	},
	createMaterialUpdateLog: async function (material, transaction) {

		const query =
			`	INSERT INTO dbo.MaterialUpdateLog
			(MaterialId, CreationDate, MaterialCategoryId, MaterialTypeId, MaterialFormatId, ThumbnailUrl, Name, Description, UserId, OwnerId, LanguageId, CountryId, CourseId, UniversityId,  SchoolId, AreaId, StudentTypeId, StudentDegreeLevelId, PDAppClientId, ClientIp, Subjects, Period)
			VALUES	(:Id, UTC_TIMESTAMP, :CategoryTypeId, :MaterialTypeId, :MaterialFormatId, :Thumbnail, :Title, :Description, :UserId, :UserId, :LanguageId,:CountryId, :CourseId, :UniversityId, :SchoolId, :AreaId, :StudentTypeId, :StudentDegreeLevelId, :PdAppClientId, :IpAddress, :SubjectIds, :Period);`


		var connection = await dbClient.getConnectionAsync(transaction);
		var result = await connection.queryAsync(query, material);
		return result[0];
	}


}


module.exports = materialRepository;