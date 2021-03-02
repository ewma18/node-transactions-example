const Promise = require('bluebird'),
	_ = require('lodash'),
	spamWhitelist = require('../config.js').getAppParameters().spamWhitelist,
	errors = require('restify-errors'),
	onboardingQueueClient = require('../infra/rabbitMQ/onboardingQueueClient'),
	queueService = require('./queueService.js');

const Transaction = require('./transaction');;
const dbConfig = require('../config.js').getConnections().mysql;;

const DOCUMENT_TYPE = require('../constant/documentType'),
	MATERIAL_TYPE = require('../constant/materialType'),
	COUNTRY = require('../constant/country'),
	LANGUAGE = require('../constant/language'),
	MATERIAL_FORMAT = require('../constant/materialFormat'),
	contentObjectType = require('../constant/contentObjectType');

const tagServices = require('./tagServices'),
	utilsService = require('./utilsService');

const Question = require('../domain/material').Question;

const CATEGORY_OTHER = 7;

const subjectRepository = require('../infra/mySql/subjectRepository'),
	materialRepository = require('../infra/mySql/materialRepository'),
	helperRepository = require('../infra/mySql/helperRepository'),
	questionRepository = require('../infra/mySql/questionRepository');

const questionService = {
	checkAndSendMaterial: async function (material) {
		console.log('Start persist Question');

		let transaction = new Transaction(dbConfig);

		try {
			await transaction.beginTransactionAsync();

			if (!spamWhitelist.includes(material.UserId)) {
				await this.spamQuestionCheck(material);
			}

			material.Id = await helperRepository.createContentObject(contentObjectType.QUESTION, transaction);

			let UserInfo = await helperRepository.getUserInfo(material.UserId);

			let QuestionMaterial = this.mountQuestionMaterial(_.merge(material, UserInfo));

			await materialRepository.createMaterial(QuestionMaterial, transaction);
			await questionRepository.createQuestion(QuestionMaterial, transaction);

			if (QuestionMaterial.SubjectIds) {
				for (let SubjectId of QuestionMaterial.SubjectIds.split(';')) {
					await materialRepository.createMaterialSubject(QuestionMaterial.Id, SubjectId, transaction);
					await subjectRepository.incrementMaterialCountAsync(SubjectId, transaction);
				}
			}

			await materialRepository.createMaterialUpdateLog(QuestionMaterial, transaction);

			if (QuestionMaterial.Tags.length > 0) {
				console.log("Creating tags Material [" + QuestionMaterial.Title + "]");
				await tagServices.createTags(QuestionMaterial.Tags, QuestionMaterial, transaction)
			}

            console.log("Commiting changes"); 
			await transaction.commitAsync();

			console.log("Queue Material-Question [" + QuestionMaterial.Title + "]");

			await Promise.all([
				onboardingQueueClient.publishAsync({
					StudentId: QuestionMaterial.UserId
				}),
				queueService.createMaterialAsync(QuestionMaterial.Id, DOCUMENT_TYPE.Question, MATERIAL_TYPE.QUESTION)
			]);

			console.log("All ok Material-Question [" + QuestionMaterial.Title + "]");

			return QuestionMaterial.Id;

		} catch (err) {
			if (transaction) {
				transaction.rollbackAsync();
			}
			throw err;
		}
	},
	mountQuestionMaterial: function (material) {
		return material = new Question(material.Id,
			material.Tags,
			material.Title,
			material.Description,
			material.UserId,
			material.MaterialListIds,
			MATERIAL_TYPE.QUESTION,
			MATERIAL_FORMAT.TEXT,
			LANGUAGE.PORTUGUESE,
			COUNTRY.BRAZIL,
			material.CourseId,
			material.SubjectIds,
			CATEGORY_OTHER,
			material.StudentTypeId,
			material.StudentDegreeLevelId,
			material.UniversityId,
			material.SchoolId,
			material.AreaId,
			material.ThumbnailUrl,
			material.ClientIp,
			material.PdAppClientId,
			material.PdAppClientVersion,
			material.Period,
			material.IsPremium,
			material.OwnerId,
			material.Text,
			material.ModerationType);
	},
	spamQuestionCheck: async function (material) {
		let lastQuestion = await questionRepository.getLastQuestion(material.UserId);
		if (lastQuestion != null &&
			(utilsService.levenshteinDistance(lastQuestion.Title, material.Title) ||
				(material.Text != "" && lastQuestion.Text != "" && utilsService.levenshteinDistance(lastQuestion.Text, material.Text)))) {
			throw new errors.ConflictError("Question too similar");
		}
	}
};
module.exports = questionService;