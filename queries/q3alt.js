db.raw.aggregate([

    //Group by the vehicle
    //Goal is find the distance each vehicle has driven within the time window, along with the actual path it traversed
    //At the end, return the vehicle(s) and the paths with the greatest distance
    //==========================================================================================================
	{
		$group:
		{
			_id: "$name",
			docs: {$push: "$$ROOT"}
		}
	},
	{
		$sort: {time: 1}
	},

    //We have a SORTED array of all the entries, for each car
    //Change array elements, so that they only contain:
    //
    //  name: Name of the vehicle
    //  time: The timestamp
    //  link: The current link
    //  position: Position within the current link
    //  next: Information about the next document in the array (5 seconds from now)
    //      link: The next link
    //      position: Position within the next link
    //      dista_until_next_timestamp: The distance I'll drive between now and the next moment. THIS IS WHAT GETS SUMMED UP
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
                        //Current entry information
                        name: {$arrayElemAt: ["$docs.name", "$$i"]},
						time: {$arrayElemAt: ["$docs.time", "$$i"]},
						link: {$arrayElemAt: ["$docs.link", "$$i"]},
						position: {$arrayElemAt: ["$docs.position", "$$i"]},

                        //Next entry information
						next:
						{
							$cond:
							{
                                //The last document
                                //The next entry will be about the same link (since I'm on my destination link)(Am I????)
                                //The position will be the end of the link (500)
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
										if: //I stay within the same link between timestamps
										{
											$eq:
											[
												{$arrayElemAt: ["$docs.link", "$$i"] },
												{$arrayElemAt: ["$docs.link", {$add: ["$$i", 1]} ] }
											]
										},
										then: //Just subtract the two positions
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
										else: //Add distance from current position to the end of the current link, plus the remaining distance
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

    //Limit time
    //Why am I using "<" instead of "<="?
    //Because from each array element, we will use the "distance until THE NEXT timestamp" for the sum
    //During this next timestamp, we will be in our final position in the specified time window
    //So we don't want the entry for that specific timestamp. We don't care about its next
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
							{$lt: ["$$elem.time", new ISODate('2024-06-21T21:10:29.000Z')]}
						]
					}
				}
			}
		}
	},
	{
		$unwind: "$filteredArray"
	},
	{
		$replaceRoot: {newRoot: "$filteredArray"}
	},

    //Calculate final dista for each vehicle
    //==========================================================================================================
    {
		$group:
		{
			_id: "$name",
			docs: {$push: "$$ROOT"},
			dista_travelled: {$sum: "$next.dista_until_next_timestamp"}
		}
	},

    //EVERYTHING AFTER THIS POINT IS ABOUT FORMATTING THE PATH

    //For each vehicle, find its last link within the time window
    //For each array element, mention its position using the constants "first", "last", "middle", "firstlast" (first & last)
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
								if: {$eq: ["$$i", 0]}, //Element is first
								then:
								{
									$cond:
									{
										if: {$eq: ["$$i", {$subtract: [{$size: "$docs"}, 1]}]}, //Element is both first and last
										then: {index: "firstlast"},
										else: {index: "first"}
									}
								},
								else: //Element is not first
								{
									$cond:
									{
										if: {$eq: ["$$i", {$subtract: [{$size: "$docs"}, 1]}]}, //Element is last
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
								$cond:
								{
									if: {$eq: ["$$elem.link", "$$elem.next.link"]},
									then:
									{
										$concat: ["$$elem.link", "(", {$toString: "$$elem.position"}, ")"]
									},
									else:
									{
										$concat: ["$$elem.link", "(", {$toString: "$$elem.position"}, ")", " - ", "$$elem.next.link"]
									}
								}
								
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
	},
    {
        $group:
        {
            _id: "$dista_travelled",
            data: {$push: "$$ROOT"}
        }
    },
    {
        $sort:
        {
            _id: -1
        }
    },
    {
        $limit: 1
    }
])