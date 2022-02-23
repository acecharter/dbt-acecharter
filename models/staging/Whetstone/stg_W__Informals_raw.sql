SELECT
  _id AS InformalsId
  tags._id AS TagsId
  tags.name AS TagName
  shared AS Shared
  private AS Private
  user._id AS UserId,
  user.email AS UserEmail,
  user.name AS UserName,
  creator._id AS CreatorId,
  creator.email AS CreatorEmail,
  creator.name AS CreatorName,
  district AS District
  created AS CreatedTimestamp
  lastModified AS LastModifiedTimestamp
  
FROM {{ source('Whetstone', 'Informals_raw')}}