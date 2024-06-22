db.raw.aggregate([
	{
		$match: {name: "28"} //<3
	},
	{
		$sort: {time: 1}
	},

	//Throw all matching documents into an array, to perform mapping operations on them
	//==========================================================================================================
	{
		$group:
		{
			_id: null,
			docs: {$push: "$$ROOT"},
		}
	},

	//MAPPING
	//Keep time, link and position
	//Add an extra field called "next", which contains information about the next
	//==========================================================================================================
	{
		$project:
		{
			mappedArray:
			{
				$map:
				{
					input: {$range: [0, {$size: "$docs"}]},
					as: "i",
					in:
					{
						time: {$arrayElemAt: ["$docs.time", "$$i"]},
						link: {$arrayElemAt: ["$docs.link", "$$i"]},
						position: {$arrayElemAt: ["$docs.position", "$$i"]},
						next:
						{
							$cond:
							{
								if: {$eq: ["$$i", {$subtract: [{$size: "$docs"}, 1]}]},
								then:
								{
									position: 500,
									link: {$arrayElemAt: ["$docs.link", "$$i"]},
									dista_until_next_timestamp: {$subtract: [500, {$arrayElemAt: ["$docs.position", "$$i"]}]}
								},
								else:
								{
									$cond:
									{
										if:
										{
											$eq:
											[
												{$arrayElemAt: ["$docs.link", "$$i"] },
												{$arrayElemAt: ["$docs.link", {$add: ["$$i", 1]} ] }
											]
										},
										then:
										{
											position: {$arrayElemAt: ["$docs.position", {$add: ["$$i", 1]} ] },
											link: {$arrayElemAt: ["$docs.link", {$add: ["$$i", 1]} ] },
											dista_until_next_timestamp:
											{
												$subtract:
												[
													{$arrayElemAt: ["$docs.position", {$add: ["$$i", 1]} ] },
													{$arrayElemAt: ["$docs.position", "$$i"]}
												]
											}
										},
										else:
										{
											position: {$arrayElemAt: ["$docs.position", {$add: ["$$i", 1]} ] },
											link: {$arrayElemAt: ["$docs.link", {$add: ["$$i", 1]} ] },
											dista_until_next_timestamp:
											{
												$add:
												[
													{$subtract: [500, {$arrayElemAt: ["$docs.position", "$$i"]}]},
													{$arrayElemAt: ["$docs.position", {$add: ["$$i", 1]}]}
												]
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
	},

	//==========================================================================================================
	{
		$project:
		{
			filteredArray:
			{	
				$filter:
				{
					input: "$mappedArray",
					as: "elem",
					cond:
					{
						$and:
						[
							{$gte: ["$$elem.time", new ISODate('2024-06-21T21:09:09.000Z')]},
							{$lt: ["$$elem.time", new ISODate('2024-06-21T21:10:24.000Z')]}
						]
					}
				}
			}
		}
	},

	//==========================================================================================================
	{
		$unwind: "$filteredArray"
	},
	{
		$replaceRoot: {newRoot: "$filteredArray"}
	},
	{
		$group:
		{
			_id: null,
			docs: {$push: "$$ROOT"},
			dista_travelled: {$sum: "$next.dista_until_next_timestamp"}
		}
	},

	//==========================================================================================================
	{
		$project:
		{
			dista_travelled: 1,
			last_link: {$arrayElemAt: ["$docs.next.link", -1]},
			temp:
			{
				$map:
				{
					input: {$range: [0, {$size: "$docs"}]},
					as: "i",
					in:
					{
						$mergeObjects: [{$arrayElemAt: ["$docs", "$$i"]},
						{
							$cond:
							{
								if: {$eq: ["$$i", 0]},
								then:
								{
									$cond:
									{
										if: {$eq: ["$$i", {$subtract: [{$size: "$docs"}, 1]}]},
										then: {index: "firstlast"},
										else: {index: "first"}
									}
								},
								else:
								{
									$cond:
									{
										if: {$eq: ["$$i", {$subtract: [{$size: "$docs"}, 1]}]},
										then: {index: "last"},
										else:{index: "middle"}
									}
								}
							}
						}]
					}
				}
			}
		}
	},

	//==========================================================================================================
	{
		$project:
		{
			dista_travelled: 1,
			last_link: 1,
			path:
			{
				$filter:
				{
					input: "$temp",
					as: "elem",
					cond:
					{
						$or:
						[
							{$eq: ["$$elem.index", "first"]},
							{$eq: ["$$elem.index", "last"]},
							{$eq: ["$$elem.index", "firstlast"]},
							{
								$and:
								[
									{$ne: ["$$elem.link", "$$elem.next.link"]},
									{$ne: ["$$elem.index", "last"]}
								]
								
							}
						]
					}
				}
			}
		}
	},

	//==========================================================================================================
	{
		$project:
		{
			dista_travelled: 1,
			last_link: 1,
			path:
			{
				$map:
				{
					input: "$path",
					as: "elem",
					in:
					{
						$cond:
						{
							if: {$eq: ["$$elem.index", "first"]},
							then:
							{
								$concat: ["$$elem.link", "(", {$toString: "$$elem.position"}, ")"]
							},
							else:
							{
								$cond:
								{
									if: {$eq: ["$$elem.index", "firstlast"]},
									then:
									{
										$concat: ["$$elem.link", "(", {$toString: "$$elem.position"}, ")", " - ", "$$elem.next.link", "(", {$toString: "$$elem.next.position"}, ")"]
									},
									else:
									{
										$cond:
										{
											if: {$eq: ["$$elem.index", "last"]},
											then:
											{
												$concat: ["$$elem.next.link", "(", {$toString: "$$elem.next.position"}, ")"]
											},
											else: "$$elem.next.link"
										}
									}
								}
							}						
						}
					}
				}
			}
		}
	},

	//==========================================================================================================
	{
		$project:
		{
			dista_travelled: 1,
			path:
			{
				$filter:
				{
					input: "$path",
					as: "elem",
					cond:
					{
						$ne: ["$$elem", "$last_link"]
					}
				}
			}
		}
	},

	//==========================================================================================================
	{
		$project:
		{
			dista_travelled: 1,
			path:
			{
				$reduce:
				{
					input: "$path",
					initialValue: "",
					in:
					{
						$concat:
						[
							{
								$cond:
								{
									if: {$ne: ["$$value", ""]},
									then: {$concat: ["$$value", " - "]},
									else: ""
								}
							},
							"$$this"
						]
					}
				}
			}
		}
	}
])

