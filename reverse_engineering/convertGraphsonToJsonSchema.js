
const getType = rawType => {
	switch(rawType) {
		case 'g:List':
			return { type: 'list' };
		case 'g:Map':
			return { type: 'map' };
		case 'g:Set':
			return { type: 'set' };
		case 'g:Double':
			return { type: 'number', mode: 'double' };
		case 'gx:Byte':
			return { type: 'number', mode: 'byte' };
		case 'gx:Int16':
			return { type: 'number', mode: 'short' };
		case 'g:Int32':
			return { type: 'number', mode: 'integer' };
		case 'g:Int64':
			return { type: 'number', mode: 'long' };
		case 'g:Float':
			return { type: 'number', mode: 'float' };
		case 'g:Date':
			return { type: 'date' };
		default: {
			return { type: 'map' };
		}
	}
};

const getValue = (item) => {
	if (isPlainObject(item)) {
		return item['@value'];
	} else {
		return item;
	}
};

const groupPropertiesForMap = properties => {
	const { keys, values} = properties.reduce(({keys, values}, property, index) => {
		if (index % 2) {
			return { keys, values: [ ...values, convertGraphSonToJsonSchema(property)] };
		}

		return { keys: [ ...keys, getValue(property) + ''], values };
	}, {
		keys: [],
		values: []
	});

	return keys.reduce((properties, key, index) => {
		return Object.assign({}, properties, {
			[key]: values[index] || {}
		});
	}, {});
};
const getUniqItems = (items) => {
	return items.reduce((result, item) => {
		const exists = result.find(existed => existed.type === item.type && existed.mode === item.mode);

		if (exists) {
			return result;
		}

		return result.concat(item);
	}, []);
};

const getItems = properties => properties.map(convertGraphSonToJsonSchema);

const isPlainObject = (obj) => obj && typeof obj === 'object';

const getDateSample = (date) => {
	const year = date.getFullYear();
	const month = ((date.getMonth() + 1) + '').padStart(2, '0');
	const day = (date.getDate() + '').padStart(2, '0');
	const hours = (date.getHours() + '').padStart(2, '0');
	const minutes = (date.getMinutes() + '').padStart(2, '0');
	const seconds = (date.getSeconds() + '').padStart(2, '0');

	return `${year}-${month}-${day}T${hours}:${minutes}:${seconds}`;
};

const getSample = (type, value) => {
	if (type === 'date') {
		return getDateSample(new Date(value));
	} else if (Array.isArray(value)) {
		return getSample(value[0]);
	} else {
		return value;
	}
};

const convertGraphSonToJsonSchema = (graphSON) => {
	if (!isPlainObject(graphSON)) {
		return {
			type: typeof graphSON,
			sample: graphSON
		};
	}

	const rawType = graphSON['@type'];
	const { type, mode } = getType(rawType);
	const rawProperties = graphSON['@value'];

	if (rawType === 'g:Map') {
		const properties = groupPropertiesForMap(rawProperties);

		return { type, properties }
	}

	if (rawType === 'g:List' || rawType === 'g:Set') {
		const items = getItems(rawProperties);

		if (items.length === 1) {
			return items[0];
		}

		const propCardinality = items.length > 1 ? 'set' : 'single';
		const uniqueItems = getUniqItems(items);
		
		if (uniqueItems.length === 1) {
			return {
				...uniqueItems,
				propCardinality,
			};
		}

		return {
			type: 'multi-property',
			items: uniqueItems,
		};
	}

	if (mode) {
		return { type, mode, sample: getSample(type, rawProperties) }
	}

	return { type, sample: getSample(type, rawProperties) };
};

module.exports = convertGraphSonToJsonSchema;
