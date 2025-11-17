const sql = require("mssql");

// Campos que chegam com JSON interno quebrado (aspas dentro de aspas)
const CAMPOS_SUJOS = [
  "brandPurchasedTag",
  "brandVisitedTag",
  "categoryPurchasedTag",
  "categoryVisitedTag",
  "departmentVisitedTag",
  "productPurchasedTag",
  "productVisitedTag",
  "visitedProductWithStockOutSkusTag",
  "carttag",
  "checkouttag",
];

function limparCamposSujos(bodyStr) {
  // Para cada campo, substitui o valor inteiro por string vazia ("")
  CAMPOS_SUJOS.forEach((campo) => {
    // casa algo como:
    // "campo": "qualquer coisa até a próxima aspa e vírgula",
    const padrao = new RegExp(`"${campo}"\\s*:\\s*"[\\s\\S]*?",`, "g");
    bodyStr = bodyStr.replace(padrao, `"${campo}": "",`);
  });
  return bodyStr;
}

function toBool(value) {
  if (typeof value === "boolean") return value ? 1 : 0;
  if (typeof value === "string") {
    const v = value.toLowerCase();
    if (v === "true") return 1;
    if (v === "false") return 0;
  }
  return null;
}

function toGuidOrNull(value) {
  if (typeof value === "string" && /^[0-9a-fA-F-]{36}$/.test(value)) {
    return value;
  }
  return null;
}

// Config Azure SQL (VTEX)
const dbConfig = {
  user: "sa",
  password: process.env.VTEX_DB_PASSWORD,
  server: "31.97.85.55",
  database: "master",
  options: {
    encrypt: true,
    trustServerCertificate: true
  }
};

exports.handler = async (event) => {
  if (event.httpMethod !== "POST") {
    return {
      statusCode: 405,
      body: JSON.stringify({ message: "Método não permitido" })
    };
  }

  const raw = event.body || "";
  console.log("RAW recebido:", raw);

  // 1) Limpa campos sujos pra conseguir fazer JSON.parse
  const bodyLimpo = limparCamposSujos(raw);
  console.log("BODY LIMPO:", bodyLimpo);

  let data;
  try {
    data = JSON.parse(bodyLimpo);
  } catch (err) {
    console.error("Erro ao fazer JSON.parse:", err.message);
    return {
      statusCode: 400,
      body: JSON.stringify({
        status: "erro_json",
        detail: err.message
      })
    };
  }

  console.log("Email:", data.email);
  console.log("Último carrinho:", data.rclastcart);
  console.log("Valor carrinho:", data.rclastcartvalue);

  // 2) Conecta no banco e insere
  if (!process.env.VTEX_DB_PASSWORD) {
    return {
      statusCode: 500,
      body: JSON.stringify({
        status: "erro_banco",
        detail: "VTEX_DB_PASSWORD não configurada no Netlify"
      })
    };
  }

  let pool;
  try {
    pool = await sql.connect(dbConfig);

    const request = pool.request();

    // Monta parâmetros (mesma ordem do INSERT do FastAPI)
    request
      .input("MasterDataId", sql.UniqueIdentifier, toGuidOrNull(data.id))
      .input("UserId", sql.UniqueIdentifier, toGuidOrNull(data.userId))
      .input("Email", sql.NVarChar(255), data.email || null)
      .input("FirstName", sql.NVarChar(100), data.firstName || null)
      .input("LastName", sql.NVarChar(100), data.lastName || null)
      .input("Document", sql.NVarChar(50), data.document || null)
      .input("DocumentType", sql.NVarChar(20), data.documentType || null)

      .input("IsNewsletterOptIn", sql.Bit, toBool(data.isNewsletterOptIn))

      .input("Phone", sql.NVarChar(30), data.phone || null)
      .input("HomePhone", sql.NVarChar(30), data.homePhone || null)
      .input("BusinessPhone", sql.NVarChar(30), data.businessPhone || null)

      .input("BirthDate", sql.NVarChar(30), data.birthDate || null) // deixa o SQL converter
      .input("BirthDateMonth", sql.TinyInt, data.birthDateMonth || null)

      .input("RCLastCart", sql.NVarChar(sql.MAX), data.rclastcart || null)
      .input("RCLastCartValue", sql.Decimal(18, 2),
        data.rclastcartvalue ? Number(data.rclastcartvalue) : null
      )
      .input("RCLastSession", sql.UniqueIdentifier, toGuidOrNull(data.rclastsession))
      .input("RCLastSessionDate", sql.NVarChar(30), data.rclastsessiondate || null)

      .input("CartTag", sql.NVarChar(sql.MAX), data.carttag || null)
      .input("CheckoutTag", sql.NVarChar(sql.MAX), data.checkouttag || null)

      .input("Cluster", sql.NVarChar(100), data.cluster || null)
      .input("ClusterFreteGratis", sql.NVarChar(100), data.clusterfretegratis || null)
      .input("ClusterVIP", sql.NVarChar(100), data.ClusterVIP || null)
      .input("Funcionario", sql.NVarChar(10), data.funcionario || null)
      .input("Gender", sql.NVarChar(20), data.gender || null)
      .input("TradeName", sql.NVarChar(200), data.tradeName || null)

      .input("IsCorporate", sql.Bit, toBool(data.isCorporate))
      .input("CorporateName", sql.NVarChar(200), data.corporateName || null)
      .input("CorporateDocument", sql.NVarChar(50), data.corporateDocument || null)
      .input("LocaleDefault", sql.NVarChar(10), data.localeDefault || null)
      .input("StateRegistration", sql.NVarChar(50), data.stateRegistration || null)

      .input("CustomerClass", sql.NVarChar(50), data.customerClass || null)
      .input("PriceTables", sql.NVarChar(200), data.priceTables || null)
      .input("TradePolicy", sql.NVarChar(50), data.tradePolicy || null)

      .input("AccountId", sql.UniqueIdentifier, toGuidOrNull(data.accountId))
      .input("AccountName", sql.NVarChar(100), data.accountName || null)
      .input("DataEntityId", sql.NVarChar(20), data.dataEntityId || null)
      .input("CreatedBy", sql.NVarChar(100), data.createdBy || null)
      .input("CreatedIn", sql.NVarChar(30), data.createdIn || null)
      .input("UpdatedBy", sql.NVarChar(100), data.updatedBy || null)
      .input("UpdatedIn", sql.NVarChar(30), data.updatedIn || null)

      .input("LastInteractionBy", sql.NVarChar(100), data.lastInteractionBy || null)
      .input("LastInteractionIn", sql.NVarChar(30), data.lastInteractionIn || null)
      .input("Followers", sql.NVarChar(sql.MAX), data.followers || null)
      .input("Tags", sql.NVarChar(sql.MAX), data.tags || null)
      .input("AutoFilter", sql.NVarChar(100), data.auto_filter || null)

      .input("HtmlUrl", sql.NVarChar(500), data.html_url || null)
      .input("ProfilePicture", sql.NVarChar(500), data.profilePicture || null)

      .input("RawJson", sql.NVarChar(sql.MAX), raw);

    const query = `
      INSERT INTO dbo.AbandonedCartEvents (
        MasterDataId, UserId, Email, FirstName, LastName, Document, DocumentType,
        IsNewsletterOptIn, Phone, HomePhone, BusinessPhone,
        BirthDate, BirthDateMonth,
        RCLastCart, RCLastCartValue, RCLastSession, RCLastSessionDate,
        CartTag, CheckoutTag,
        Cluster, ClusterFreteGratis, ClusterVIP, Funcionario, Gender, TradeName,
        IsCorporate, CorporateName, CorporateDocument, LocaleDefault, StateRegistration,
        CustomerClass, PriceTables, TradePolicy,
        AccountId, AccountName, DataEntityId, CreatedBy, CreatedIn, UpdatedBy, UpdatedIn,
        LastInteractionBy, LastInteractionIn, Followers, Tags, AutoFilter,
        HtmlUrl, ProfilePicture, RawJson
      )
      VALUES (
        @MasterDataId, @UserId, @Email, @FirstName, @LastName, @Document, @DocumentType,
        @IsNewsletterOptIn, @Phone, @HomePhone, @BusinessPhone,
        @BirthDate, @BirthDateMonth,
        @RCLastCart, @RCLastCartValue, @RCLastSession, @RCLastSessionDate,
        @CartTag, @CheckoutTag,
        @Cluster, @ClusterFreteGratis, @ClusterVIP, @Funcionario, @Gender, @TradeName,
        @IsCorporate, @CorporateName, @CorporateDocument, @LocaleDefault, @StateRegistration,
        @CustomerClass, @PriceTables, @TradePolicy,
        @AccountId, @AccountName, @DataEntityId, @CreatedBy, @CreatedIn, @UpdatedBy, @UpdatedIn,
        @LastInteractionBy, @LastInteractionIn, @Followers, @Tags, @AutoFilter,
        @HtmlUrl, @ProfilePicture, @RawJson
      );
    `;

    await request.query(query);

    return {
      statusCode: 200,
      body: JSON.stringify({
        status: "ok",
        email: data.email,
        rclastcart: data.rclastcart
      })
    };

  } catch (err) {
    console.error("Erro ao salvar no banco:", err);
    return {
      statusCode: 500,
      body: JSON.stringify({
        status: "erro_banco",
        detail: err.message
      })
    };
  } finally {
    if (pool) {
      pool.close();
    }
  }
};
