'use strict';

const connectionHelper = require('./connectionHelper');
const queryHelper = require('./queryHelper');

module.exports = {
	connect: function(connectionInfo, logger){
		logger.clear();
		logger.log('info', connectionInfo, 'connectionInfo', connectionInfo.hiddenKeys);

		return connectionHelper.connect(connectionInfo);
	},

	disconnect: function(connectionInfo, cb){
		connectionHelper.close();
		cb();
	},

	testConnection: async function(connectionInfo, logger, cb){
		try {
			const connection = await this.connect(connectionInfo, logger);
			await connection.testConnection();
			this.disconnect(connectionInfo, () => {});

			cb();
		} catch (error) {
			this.disconnect(connectionInfo, () => {});
			logger.log('error', prepareError(error));
			cb({ message: 'Connection error', stack: error.stack });
		}
	},

	getDbCollectionsNames: async function(connectionInfo, logger, cb, app) {
		try {
			const connection = await this.connect(connectionInfo, logger);
			const query = queryHelper({
				_: app.require('lodash'),
				connection,
			});
			const labels = await query.getLabels();

			cb(null, [{
				dbName: 'g',
				dbCollections: labels,
			}]);
		} catch (error) {
			logger.log('error', prepareError(error));
			cb(prepareError(error));
		}
	},

	getDbCollectionsData: async function(data, logger, cb, app){
		try {
			const async = app.require('async');
			const _ = app.require('lodash');
			const connection = await this.connect(data, logger);
			const query = queryHelper({ _, connection });
	
			const collections = data.collectionData.collections;
			const dataBaseNames = data.collectionData.dataBaseNames;
			const fieldInference = data.fieldInference;
			const includeEmptyCollection = data.includeEmptyCollection;
			const includeSystemCollection = data.includeSystemCollection;
			const recordSamplingSettings = data.recordSamplingSettings;
			let packages = {
				labels: [],
				relationships: []
			};

			await async.map(dataBaseNames, async (dbName) => {
				let labels = collections[dbName];
				const labelPackages = await getNodesData({
					sampling: {
						recordSamplingSettings,
						fieldInference,
						includeEmptyCollection,
					},
					query,
					dbName,
					labels,
					logger,
					async,
					_,
				});

				packages.labels.push(labelPackages);
				labels = labelPackages.map(packageData => packageData.collectionName);

				let relationshipSchema = await query.getRelationshipSchema(labels);
				relationshipSchema = relationshipSchema.filter(data => {
					return (labels.indexOf(data.start) !== -1 && labels.indexOf(data.end) !== -1);
				});
				const relationships = await getRelationshipData({
					_,
					async,
					query,
					dbName,
					fieldInference,
					recordSamplingSettings,
					schema: relationshipSchema,
				});
				
				packages.relationships.push(relationships);
			});

			cb(null, packages.labels, {}, [].concat.apply([], packages.relationships));
		} catch (error) {
			logger.log('error', prepareError(error));
			cb(prepareError(error));
		}
	}
};

const getCount = (count, recordSamplingSettings) => {
	const per = recordSamplingSettings.relative.value;
	const size = (recordSamplingSettings.active === 'absolute')
		? recordSamplingSettings.absolute.value
		: Math.round(count / 100 * per);
	return size;
};

const isEmptyLabel = (_, documents) => {
	if (!Array.isArray(documents)) {
		return true;
	}

	return documents.reduce((result, doc) => result && _.isEmpty(doc), true);
};

const getTemplate = (_, documents, rootTemplateArray = []) => {
	const template = rootTemplateArray.reduce((template, key) => Object.assign({}, template, { [key]: {} }), {});

	if (!_.isArray(documents)) {
		return template;
	}

	return documents.reduce((tpl, doc) => _.merge(tpl, doc), template);
};

const getNodesData = async ({
	_,
	async,
	dbName,
	labels,
	logger,
	query,
	sampling,
}) => {
	const packages = await async.map(labels, async (labelName) => {
		logger.progress({ message: 'Start sampling data', containerName: dbName, entityName: labelName });
		
		const quantity = await query.getNodesCount(labelName);
		
		logger.progress({ message: 'Start getting data from graph', containerName: dbName, entityName: labelName });
		const limit = getCount(quantity, sampling.recordSamplingSettings);

		const documents = await query.getNodes(labelName, limit);
		const { schema, template } = await query.getSchema('V', documents, labelName, limit);


		logger.progress({ message: `Data has successfully got`, containerName: dbName, entityName: labelName });
		
		const packageData = getLabelPackage({
			includeEmptyCollection: sampling.includeEmptyCollection, 
			fieldInference: sampling.fieldInference,
			dbName, 
			labelName, 
			documents,
			schema,
			template,
			_,
		});

		return packageData;
	});
			
			
	const sortedPackages = sortPackagesByLabels(_, labels, packages);

	return sortedPackages;
};

const sortPackagesByLabels = (_, labels, packages) => {
	return [...packages].sort((a, b) => {
		const indexA = _.indexOf(labels, a['collectionName']);
		const indexB = _.indexOf(labels, b['collectionName']);
		if (_.isUndefined(indexA)) {
			return 1;
		}
		if (_.isUndefined(indexB)) {
			return -1;
		}

		return indexA - indexB;
	})
}

const getRelationshipData = ({
	_,
	async,
	query,
	schema,
	dbName,
	recordSamplingSettings,
	fieldInference,
}) => {
	return async.map(schema, async (chain) => {
		const quantity = await query.getCountRelationshipsData(chain.start, chain.relationship, chain.end);
		const count = getCount(quantity, recordSamplingSettings);
		const documents = await query.getRelationshipData(chain.start, chain.relationship, chain.end, count);
		const { schema, template } = await query.getSchema('E', documents, chain.relationship, count);
	
		let packageData = {
			dbName,
			parentCollection: chain.start, 
			relationshipName: chain.relationship, 
			childCollection: chain.end,
			documents,
			validation: {
				jsonSchema: schema
			}
		};

		if (fieldInference.active === 'field') {
			packageData.documentTemplate = getTemplate(_, documents, template);
		}

		return packageData;
	});
};

const getLabelPackage = ({_, dbName, labelName, documents, template, schema, includeEmptyCollection, fieldInference}) => {
	let packageData = {
		dbName,
		collectionName: labelName,
		documents,
		views: [],
		emptyBucket: false,
		validation: {
			jsonSchema: schema
		},
		bucketInfo: {
			traversalSource: dbName
		}
	};

	if (fieldInference.active === 'field') {
		packageData.documentTemplate = getTemplate(_, documents, template);
	}

	if (includeEmptyCollection || !isEmptyLabel(_, documents)) {
		return packageData;
	} else {
		return null;
	}
}; 

const prepareError = (error) => {
	return {
		message: error.message,
		stack: error.stack
	};
};
